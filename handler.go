package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/joho/godotenv"
)

var (
	database   string
	collection string
)

const (
	// environment variables
	mongoDBConnectionStringEnvVarName = "MONGODB_CONNECTION_STRING"
	mongoDBDatabaseEnvVarName         = "MONGODB_DATABASE"
	mongoDBCollectionEnvVarName       = "MONGODB_COLLECTION"
	azureStorageAccount               = "AZURE_STORAGE_ACCOUNT"
	azureStorageAccessKey             = "AZURE_STORAGE_ACCESS_KEY"
)

// define mongodb collection type
type File struct {
	ID       primitive.ObjectID `bson:"_id,omitempty"`
	LinkUrl  string             `bson:"url"`
	UUID     string             `bson:"uuid"`
	FileName string             `bson:"filename"`
}

type Upload struct {
	Status int
	Secret string
}

func handleErrors(err error) {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				fmt.Println("Received 409. Container already exists")
				return
			}
		}
		log.Fatal(err)
	}
}

// create random string
func makeRandomStr(digit uint32) (string, error) {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@!?$&#<>"

	// 乱数を生成
	b := make([]byte, digit)
	if _, err := rand.Read(b); err != nil {
		return "", errors.New("unexpected error...")
	}

	// letters からランダムに取り出して文字列を生成
	var result string
	for _, v := range b {
		// index が letters の長さに収まるように調整
		result += string(letters[int(v)%len(letters)])
	}
	return result, nil
}

// connects to MongoDB
func connect() *mongo.Client {
	mongoDBConnectionString := os.Getenv(mongoDBConnectionStringEnvVarName)
	if mongoDBConnectionString == "" {
		log.Fatal("missing environment variable: ", mongoDBConnectionStringEnvVarName)
	}
	database = os.Getenv(mongoDBCollectionEnvVarName)
	if database == "" {
		log.Fatal("missing environment variable: ", mongoDBDatabaseEnvVarName)
	}
	collection = os.Getenv(mongoDBCollectionEnvVarName)
	if collection == "" {
		log.Fatal("missing environment variable: ", mongoDBCollectionEnvVarName)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	clientOptions := options.Client().ApplyURI(mongoDBConnectionString).SetDirect(true)
	c, err := mongo.NewClient(clientOptions)
	if err != nil {
		log.Fatalf("unable to initialize connection %v", err)
	}

	err = c.Connect(ctx)

	if err != nil {
		log.Fatalf("unable to initialize connection %v", err)
	}
	err = c.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("unable to connect %v", err)
	}
	return c
}

// create a saved link and uuid
func create(url, filename string) (string, error) {
	c := connect()
	ctx := context.Background()
	defer c.Disconnect(ctx)

	fileLinkCollection := c.Database(database).Collection(collection)
	pass, err := makeRandomStr(8)
	if err != nil {
		log.Fatal(err)
		return "", err
	}

	r, err := fileLinkCollection.InsertOne(ctx, File{LinkUrl: url, UUID: pass, FileName: filename})

	if err != nil {
		log.Fatalf("failed to add todo %v", err)
		return "", err
	}
	fmt.Println("Added file link", r.InsertedID)
	return pass, nil
}

// find save link and uuid
func find(uuid string) (bson.Raw, error) {
	c := connect()
	ctx := context.Background()
	defer c.Disconnect(ctx)

	fileLinkCollection := c.Database(database).Collection(collection)
	filter := bson.D{{"uuid", uuid}}
	var doc bson.Raw
	findOptions := options.FindOne()
	err := fileLinkCollection.FindOne(ctx, filter, findOptions).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		log.Println("document not found")
		return nil, err
	}
	if err != nil {
		log.Fatal("failed to find %v", err)
		return nil, err
	}
	return doc, nil
}

// create storage client
func createStorageClient() azblob.ContainerURL {
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	containerName := "filer"
	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

	containerURL := azblob.NewContainerURL(*URL, p)

	return containerURL
}

// file upload to azure storage
func upload(fileData multipart.File, fileName string) (string, error) {
	ctx := context.Background()

	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	containerName := "filer"
	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

	containerURL := azblob.NewContainerURL(*URL, p)

	// Create a file to test the upload and download.
	fmt.Printf("Creating a file to test the upload and download\n")
	saveFile, err := os.Create(fileName)
	handleErrors(err)
	if err != nil {
		return "", err
	}
	defer saveFile.Close()

	// ファイルにデータを書き込む
	_, err = io.Copy(saveFile, fileData)
	handleErrors(err)

	// Here's how to upload a blob.
	blobURL := containerURL.NewBlockBlobURL(fileName)
	file, err := os.Open(fileName)
	handleErrors(err)
	if err != nil {
		return "", err
	}

	fmt.Printf("Uploading the file with blob name: %s\n", fileName)
	_, err = azblob.UploadFileToBlockBlob(ctx, file, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})
	handleErrors(err)
	if err != nil {
		return "", err
	}

	return blobURL.String(), nil
}

// download from azure storage
func download(fileName string) (*bytes.Buffer, error) {

	ctx := context.Background()
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
		return nil, err
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	containerName := "filer"
	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

	containerURL := azblob.NewContainerURL(*URL, p)
	blobURL := containerURL.NewBlockBlobURL(fileName)
	downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	handleErrors(err)
	if err != nil {
		return nil, err
	}

	downloadedData := &bytes.Buffer{}
	bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})

	_, err = downloadedData.ReadFrom(bodyStream)
	handleErrors(err)
	if err != nil {
		return nil, err
	}
	bodyStream.Close()

	return downloadedData, nil
}

func helloHandler(w http.ResponseWriter, r *http.Request) {
	message := "This HTTP triggered function executed successfully. Pass a name in the query string for a personalized response.\n"
	name := r.URL.Query().Get("name")
	if name != "" {
		message = fmt.Sprintf("Hello, %s. This HTTP triggered function executed successfully.\n", name)
	}
	fmt.Fprint(w, message)
}

// Upload to Azure storage
// Generate uuid password
// Azure storage link and password save to CosmosDB
// return password
func uploadHandler(w http.ResponseWriter, r *http.Request) {
	// Get file data

	fmt.Printf("upload")
	formFile, formFileHeader, err := r.FormFile("file")

	handleErrors(err)
	defer formFile.Close()

	fmt.Printf("Upload file is " + formFileHeader.Filename)

	// Get file name from FormData
	url, err := upload(formFile, formFileHeader.Filename)
	if err != nil {
		fmt.Fprint(w, http.StatusText(http.StatusBadRequest))
	}

	secret, err := create(url, formFileHeader.Filename)
	if err != nil {
		fmt.Fprint(w, http.StatusText(http.StatusBadRequest))
	}

	uploaded := Upload{http.StatusOK, secret}

	res, err := json.Marshal(uploaded)
	if err != nil {
		fmt.Fprint(w, http.StatusText(http.StatusBadRequest))
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(res)

}

// Validation password
// Download data from azure storage
func downloadHandler(w http.ResponseWriter, r *http.Request) {

	secret := r.URL.Query().Get("secret")
	if secret == "" {
		fmt.Fprint(w, http.StatusText(http.StatusBadRequest))
		return
	}

	bson, err := find(secret)
	if err != nil {
		fmt.Fprint(w, http.StatusText(http.StatusNotFound))
		return
	}

	log.Printf("Find filename: " + bson.String())

	filename, err := bson.LookupErr("filename")
	if err != nil || filename.StringValue() == "" {
		fmt.Fprint(w, http.StatusText(http.StatusBadRequest))
		return
	}

	log.Println(filename)

	data, err := download(filename.StringValue())
	if err != nil {
		fmt.Fprint(w, http.StatusText(http.StatusNotFound))
		return
	}

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(filename.StringValue()))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(data.Bytes())
}

func main() {
	listenAddr := ":8080"
	if val, ok := os.LookupEnv("FUNCTIONS_CUSTOMHANDLER_PORT"); ok {
		listenAddr = ":" + val
	}
	err := godotenv.Load(fmt.Sprintf(".env.local"))
	if err != nil {
		// .env読めなかった場合の処理
		os.Exit(-1)
	}
	http.HandleFunc("/api/HttpExample", helloHandler)
	http.HandleFunc("/api/HttpTrigger", helloHandler)
	http.HandleFunc("/api/UploadTrigger", uploadHandler)
	http.HandleFunc("/api/DownloadTrigger", downloadHandler)
	log.Printf("About to listen on %s. Go to https://127.0.0.1%s/", listenAddr, listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}

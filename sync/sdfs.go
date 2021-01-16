package fs

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"io/ioutil"

	storage "cloud.google.com/go/storage"
	"github.com/kelseyhightower/envconfig"
	"github.com/olekukonko/tablewriter"
	sdfs "github.com/opendedup/sdfs-client-go/api"
	spb "github.com/opendedup/sdfs-client-go/sdfs"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v2"
)

//Config is the configuration of the Connection
type Config struct {
	Server struct {
		ServerURL    string `yaml:"url" required:"true"`
		Password     string `yaml:"password" default:""`
		DisableTrust bool   `yaml:"disable_trust"`
	} `yaml:"server"`
	Listener struct {
		Download bool `yaml:"download"`
		Upload   bool `yaml:"upload"`
		Write    bool `yaml:"write"`
		Delete   bool `yaml:"delete"`
	} `yaml:"listener"`
	GCS struct {
		Credentials string `yaml:"credentials" envconfig:"GOOGLE_APPLICATION_CREDENTIALS"`
		BucketName  string `yaml:"bucket"`
		ProjectID   string `yaml:"projectid"`
		BasePath    string `yaml:"base_path"`
		Enabled     bool   `yaml:"enabled"`
		Region      string `yaml:"region" default:"US"`
		TempDir     string `yaml:"tempdir" default:"/tmp" `
		Retry       int    `yaml:"retry" default:"3"`
		client      *storage.Client
		bucket      *storage.BucketHandle
	}
}

//Listener is the listener object for executing on changes
type Listener struct {
	con    *sdfs.SdfsConnection
	config *Config
	debug  bool
}

//Listen captures changes and runs stuff
func (n *Listener) Listen(ctx context.Context) error {
	c := make(chan *spb.FileMessageResponse)
	go n.con.FileNotification(ctx, c)
	if n.config.GCS.Enabled {

		client, err := storage.NewClient(ctx, option.WithCredentialsFile(n.config.GCS.Credentials))

		if err != nil {
			return err
		}
		n.config.GCS.client = client
		bucket := client.Bucket(n.config.GCS.BucketName)
		n.config.GCS.bucket = bucket
		n.CreateGCSBucket(ctx)
	}
	for {
		fInfo := <-c
		if fInfo == nil {
			log.Printf("done")
			return nil
		}
		var files []string
		for _, v := range fInfo.Response {
			files = append(files, v.FilePath)
			if n.debug {
				table := tablewriter.NewWriter(os.Stdout)
				table.SetHeader([]string{"File Name", v.FileName})
				atime := time.Unix(0, v.Atime*int64(time.Millisecond))
				ctime := time.Unix(0, v.Ctime*int64(time.Millisecond))
				mtime := time.Unix(0, v.Mtime*int64(time.Millisecond))
				table.Append([]string{"File Name", v.FileName})
				if v.Type == 0 {

					table.Append([]string{"Size", strconv.FormatInt(v.Size, 10)})
					table.Append([]string{"File GUID", v.FileGuild})
					table.Append([]string{"Map GUID", v.MapGuid})
					table.Append([]string{"File Path", v.FilePath})
					table.Append([]string{"Access Time", atime.String()})
					table.Append([]string{"Create Time", ctime.String()})
					table.Append([]string{"Modifies Time", mtime.String()})
					table.Append([]string{"Execute", fmt.Sprintf("%t", v.Execute)})
					table.Append([]string{"Read", fmt.Sprintf("%t", v.Read)})
					table.Append([]string{"Read", fmt.Sprintf("%t", v.Write)})
					table.Append([]string{"Hidden", fmt.Sprintf("%t", v.Hidden)})
					table.Append([]string{"Hash Code", fmt.Sprintf("%d", v.Hashcode)})
					table.Append([]string{"ID", v.Id})
					table.Append([]string{"Importing", fmt.Sprintf("%t", v.Importing)})
					table.Append([]string{"Unix Permissions", fmt.Sprintf("%d", v.Permissions)})
					table.Append([]string{"Group ID", fmt.Sprintf("%d", v.GroupId)})
					table.Append([]string{"User ID", fmt.Sprintf("%d", v.UserId)})
					table.Append([]string{"File Open", fmt.Sprintf("%t", v.Open)})
					table.Append([]string{"Symlink", fmt.Sprintf("%t", v.Symlink)})
					table.Append([]string{"Symlink Path", fmt.Sprintf("%s", v.SymlinkPath)})
					table.Append([]string{"File Type", fmt.Sprintf("%s", v.Type)})

				} else {
					table.Append([]string{"Size", strconv.FormatInt(v.Size, 10)})
					table.Append([]string{"File Path", v.FilePath})
					table.Append([]string{"Access Time", atime.String()})
					table.Append([]string{"Create Time", ctime.String()})
					table.Append([]string{"Modifies Time", mtime.String()})
					table.Append([]string{"Execute", fmt.Sprintf("%t", v.Execute)})
					table.Append([]string{"Read", fmt.Sprintf("%t", v.Read)})
					table.Append([]string{"Read", fmt.Sprintf("%t", v.Write)})
					table.Append([]string{"Hidden", fmt.Sprintf("%t", v.Hidden)})
					table.Append([]string{"Hash Code", fmt.Sprintf("%d", v.Hashcode)})
					table.Append([]string{"Unix Permissions", fmt.Sprintf("%d", v.Permissions)})
					table.Append([]string{"Group ID", fmt.Sprintf("%d", v.GroupId)})
					table.Append([]string{"User ID", fmt.Sprintf("%d", v.UserId)})
					table.Append([]string{"Symlink", fmt.Sprintf("%t", v.Symlink)})
					table.Append([]string{"Symlink Path", fmt.Sprintf("%s", v.SymlinkPath)})
					table.Append([]string{"File Type", fmt.Sprintf("%s", v.Type)})
				}
				table.Append([]string{"Event Type", fmt.Sprintf("%s", fInfo.Action)})
				table.SetAlignment(tablewriter.ALIGN_LEFT)

				table.Render()
			}
			if fInfo.Action == spb.Syncaction_DOWNLOAD && n.config.Listener.Download {

			}
			if fInfo.Action == spb.Syncaction_UPLOAD && n.config.Listener.Upload {

			}
			if fInfo.Action == spb.Syncaction_WRITE && n.config.Listener.Write {

			}
			if fInfo.Action == spb.Syncaction_DELETE && n.config.Listener.Delete {

			}
		}
	}
}

//UploadToGCS uploads a file to GCS
func (n *Listener) UploadToGCS(ctx context.Context, resp *spb.FileInfoResponse) error {
	file, err := ioutil.TempFile(n.config.GCS.TempDir, "sdfs*")
	if err != nil {
		return err
	}
	f, err := os.Open(file.Name())
	if err != nil {
		return fmt.Errorf("Open error %s : %v", file.Name(), err)
	}
	defer f.Close()
	defer os.Remove(file.Name())

	n.con.Download(ctx, resp.FilePath, file.Name())
	wc := n.config.GCS.bucket.Object(resp.FilePath).NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return fmt.Errorf("Upload Error: %v", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Close Error: %v", err)
	}
	log.Printf("Blob %v uploaded.\n", resp.FilePath)
	return nil

}

//CreateGCSBucket creates a GCS bucket if it does not exist
func (n *Listener) CreateGCSBucket(ctx context.Context) error {
	// Setup context and client

	buckets := n.config.GCS.client.Buckets(ctx, n.config.GCS.ProjectID)
	for {
		if n.config.GCS.BucketName == "" {
			return fmt.Errorf("BucketName entered is empty")
		}
		attrs, err := buckets.Next()
		// Assume bucket not found if at Iterator end and create
		if err == iterator.Done {
			// Create bucket
			if err := n.config.GCS.bucket.Create(ctx, n.config.GCS.ProjectID, &storage.BucketAttrs{
				Location: n.config.GCS.Region,
			}); err != nil {
				return fmt.Errorf("Failed to create bucket: %v", err)
			}
			log.Printf("Bucket %v created.\n", n.config.GCS.BucketName)
			return nil
		}
		if err != nil {
			return fmt.Errorf("Issues setting up Bucket(%q) Objects(): %v Double check project id", attrs.Name, err)
		}
		if attrs.Name == n.config.GCS.BucketName {
			log.Printf("Bucket %v exists\n", n.config.GCS.BucketName)
			return nil
		}
	}
}

// ValidateConfigPath just makes sure, that the path provided is a file,
// that can be read
func ValidateConfigPath(path string) error {
	s, err := os.Stat(path)
	if err != nil {
		return err
	}
	if s.IsDir() {
		return fmt.Errorf("'%s' is a directory, not a normal file", path)
	}
	return nil
}

// NewConfig returns a new decoded Config struct
func NewConfig(configPath string) (*Config, error) {
	// Create config structure
	config := &Config{}

	// Open config file
	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// Init environmental variables
	err = envconfig.Process("", config)
	if err != nil {
		return nil, err
	}
	// Init new YAML decode
	d := yaml.NewDecoder(file)

	// Start YAML decoding from file
	if err := d.Decode(&config); err != nil {
		return nil, err
	}
	config.Server.ServerURL = strings.ToLower(config.Server.ServerURL)
	if !strings.HasPrefix(config.Server.ServerURL, "sdfs") {
		return nil, fmt.Errorf("unsupported server type %s, only supports sdfs:// or sdfss://", config.Server.ServerURL)
	}
	return config, nil
}

// NewsdfsListener returns a listener for a sdfs file system changes whose
// config file is the given config file. This node implements all NodeXxxxer
// operations available.
func NewsdfsListener(config string, debug bool) (*Listener, error) {
	var err error
	err = ValidateConfigPath(config)
	if err != nil {
		return nil, err
	}
	conf, err := NewConfig(config)
	if err != nil {
		return nil, err
	}

	sdfs.DisableTrust = conf.Server.DisableTrust
	sdfs.Password = conf.Server.Password
	con, err := sdfs.NewConnection(conf.Server.ServerURL)
	if err != nil {
		return nil, err
	}
	sdfsListener := &Listener{debug: debug, con: con, config: conf}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = con.GetVolumeInfo(ctx)
	if err != nil {
		return nil, err
	}

	return sdfsListener, nil
}

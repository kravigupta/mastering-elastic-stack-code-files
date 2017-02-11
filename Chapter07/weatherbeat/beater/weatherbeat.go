
package beater

import (
	"fmt"
	"time"
  "io/ioutil"
	"net/http"
	"encoding/json"
	//"strconv"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/packt/weatherbeat/config"
)

type Weatherbeat struct {
	done   chan struct{}
	config config.Config
	client publisher.Client
}

// Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Weatherbeat{
		done: make(chan struct{}),
		config: config,
	}
	return bt, nil
}

func (bt *Weatherbeat) Run(b *beat.Beat) error {
	logp.Info("weatherbeat is running! Hit CTRL-C to stop it.")

	bt.client = b.Publisher.Connect()
	ticker := time.NewTicker(bt.config.Period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		cityName:= bt.config.CityName
		key := bt.config.Key
		url := "http://api.openweathermap.org/data/2.5/weather?q="+cityName+"&appid="+key
		fmt.Println("URL is " + url)

		resp, err := http.Get(url)
		if err != nil {
				fmt.Println("Something went wrong")
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)

		Message := (*json.RawMessage)(&body)
		var weatherData WeatherDataTypes
	  json.Unmarshal(*Message, &weatherData)

		event := common.MapStr{
			"@timestamp": common.Time(time.Now()),
			"type":       b.Name,
			"sunrise": 		weatherData.Sys.Sunrise,
			"sunset": 		weatherData.Sys.Sunset,
			"temp": 			weatherData.Main.Temp,
		}
		bt.client.PublishEvent(event)
		logp.Info("Event sent")

	}
}

type WeatherDataTypes struct {
    Coord   CoordTypes
    Weather   []WeatherTypes
    Base   string
    Main   MainTypes
    Visibility   int
		Wind   WindTypes
		Clouds   CloudTypes
		Dt   int
		Sys  SysTypes
		Id   int
		Name   string
		Cod	int
}
type CoordTypes struct {
    Lat   float64
    Lon 	float64
}
type MainTypes struct {
    Temp  float64
    Pressure 	int
		Humidity int
		Temp_min	float64
		Temp_max float64
}
type WindTypes struct {
    Speed  float64
    Degree 	int
}
type CloudTypes struct {
    All  int
}
type SysTypes struct {
    Id 	int
		Message	float64
		Country string
		Sunrise int
		Sunset int
}
type WeatherTypes struct {
    Id     int
    Main 	string
		Description string
		Icon	string
}

func (bt *Weatherbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

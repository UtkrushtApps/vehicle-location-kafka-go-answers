package config

type Config struct {
	HTTPAddr      string
	KafkaBrokers  []string
	LocationTopic string
}

func Local() Config {
	return Config{
		HTTPAddr:      ":8080",
		KafkaBrokers:  []string{"kafka:9092"},
		LocationTopic: "vehicle-location.v1",
	}
}

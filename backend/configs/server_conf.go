package configs

type ServerConfig struct {
	Infoside                Side
	Address                 int    `yaml:"address"`
	Host                    string `yaml:"host"`
	SessionTimeoutSeconds   int    `yaml:"session-timeout"`
	HeartbeatPollingSeconds int    `yaml:"heartbeat-polling"`
}

func DefServerConf() ServerConfig {
	return ServerConfig{
		Infoside:                "server",
		Address:                 8080,
		Host:                    "localhost",
		SessionTimeoutSeconds:   30,
		HeartbeatPollingSeconds: DefHeartbeatSeconds,
	}
}

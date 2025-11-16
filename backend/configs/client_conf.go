package configs

type ClientConfig struct {
	Infoside                Side
	Address                 int    `yaml:"address"`
	Host                    string `yaml:"host"`
	HeartbeatPollingSeconds int    `yaml:"heartbeat-polling"`
}

func DefClientConf() ClientConfig {

	return ClientConfig{
		Infoside:                "client",
		Address:                 8081,
		Host:                    "localhost",
		HeartbeatPollingSeconds: DefHeartbeatSeconds,
	}
}

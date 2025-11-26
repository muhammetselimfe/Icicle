package registrysyncer

type ChainRegistry struct {
	SubnetID    string   `json:"subnetId"`
	Network     string   `json:"network"`
	Categories  []string `json:"categories"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Logo        string   `json:"logo"`
	Website     string   `json:"website"`
}


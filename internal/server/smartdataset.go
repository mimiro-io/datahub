package server

type SmartDatasetConfig struct {
	SourceDataset     string             `json:"dataset"`
	Transform         []byte             `json:"transform"`
	Shape             []*PropertyShape   `json:"shape"`
	Filters           []*EntityFilter    `json:"filters"`
	ValidationControl *ValidationControl `json:"validationControl"`
}

type ValidationControl struct {
	Type      string `json:"validationType"`        // strict or open
	Flow      string `json:"validationControlFlow"` // stop or annotate
	SchemaURL string `json:"schemaUrl"`
}

type EntityFilter struct {
	Property string `json:"property"`
	Operator string `json:"operator"`
	Datatype string `json:"datatype"`
	Value    string `json:"value"`
}

type PropertyShape struct {
	Property       string   `json:"property"`
	ValueFunctions []string `json:"valueFunctions"`
	TargetProperty string   `json:"targetProperty"`
	IsRef          bool     `json:"isRef"`
	RefTemplate    string   `json:"refTemplate"`
	Datatype       string   `json:"datatype"`
	Required       bool     `json:"required"`
	Default        string   `json:"default"`
}

type SmartDataset struct {
	ID                 string `json:"id"`
	SmartDatasetConfig *SmartDatasetConfig
}

func NewSmartDataset(id string, config *SmartDatasetConfig) *SmartDataset {
	return &SmartDataset{
		ID:                 id,
		SmartDatasetConfig: config,
	}
}

func (sd *SmartDataset) GetChanges(since string, limit int, emit func(entity *Entity) error) error {

	// get dataset

	// get changes from dataset

	// apply filter

	// if validation control not nil then do validation

	// if transform not nil then do transform

	// if shape not nil then do shape

	return nil
}

package source

import (
	"errors"
	"fmt"
	"strconv"
)

type DependencyRegistry interface {
	Hop(nextDataset string, predicate string) DependencyRegistry
	IHop(nextDataset string, predicate string) DependencyRegistry
}

// ParseDependencies populates MultiSource dependencies based on given json config
func (multiSource *MultiSource) ParseDependencies(
	dependenciesConfig interface{},
	transformConfig map[string]any,
) error {
	dataset := multiSource.DatasetManager.GetDataset(multiSource.DatasetName)
	if dataset != nil && dataset.IsProxy() {
		return fmt.Errorf("main dataset multiSource must not be a proxy dataset: %v", multiSource.DatasetName)
	}

	// 1. Parse explicit config in job config
	if dependenciesConfig != nil {
		if depsList, ok := dependenciesConfig.([]interface{}); ok {
			for _, dep := range depsList {
				if m, ok := dep.(map[string]interface{}); ok {
					newDep := Dependency{}
					newDep.Dataset = m["dataset"].(string)
					depDataset := multiSource.DatasetManager.GetDataset(newDep.Dataset)
					if depDataset != nil && depDataset.IsProxy() {
						return fmt.Errorf(
							"dependency dataset %v in multiSource %v must not be a proxy dataset",
							newDep.Dataset,
							multiSource.DatasetName,
						)
					}

					for _, j := range m["joins"].([]interface{}) {
						newJoin := Join{}
						m := j.(map[string]interface{})
						newJoin.Dataset = m["dataset"].(string)
						newJoin.Predicate = m["predicate"].(string)
						newJoin.Inverse = m["inverse"].(bool)
						newDep.Joins = append(newDep.Joins, newJoin)
					}
					multiSource.Dependencies = append(multiSource.Dependencies, newDep)
				} else {
					return fmt.Errorf("dependency %+v must be json object structure, but is %t ", dep, dep)
				}
			}
		} else {
			return errors.New("dependenciesConfig must be array array")
		}
	}
	// 2. Add dependency tracking from transform code
	if transformConfig != nil && transformConfig["Type"] == "JavascriptTransform" && transformConfig["Code"] != nil {
		multiSource.depRegistries = []DependencyRegistry{}
		if multiSource.AddTransformDeps == nil {
			return errors.New("AddTransformDeps function must be injected during MultiSource initialization")
		}
		code64 := transformConfig["Code"]
		// AddTransformDeps function definition must be injected during MultiSource initialization,
		// avoiding circular dependency
		// AddTransformDeps calls the js function "track_queries" in transform code
		err := multiSource.AddTransformDeps(code64.(string), multiSource)
		if err != nil {
			return err
		}
		/*
			`[{"dataset": "product",
			   "joins": [{"dataset": "order", "predicate": "product-ordered", "inverse": true},
			             {"dataset": "person", "predicate": "ordering-customer", "inverse": false}]}]`,
		*/
		depConfig := []any{}
		for _, dep := range multiSource.depRegistries {
			o := map[string]any{}
			o["joins"] = make([]any, 0)
			join := dep.(*DependencyRegistryJoin)
			prevDs := multiSource.DatasetName
			for {
				insertArr := []any{map[string]any{
					"dataset":   prevDs,
					"predicate": join.Predicate,
					"inverse":   !join.Inverse,
				}}
				prevDs = join.Dataset
				o["joins"] = append(insertArr, o["joins"].([]any)...)
				if join.next == nil {
					break
				}
				join = join.next
			}
			o["dataset"] = join.Dataset
			depConfig = append(depConfig, o)
		}
		err = multiSource.ParseDependencies(depConfig, nil)
		if err != nil {
			return err
		}
		// fmt.Println(multiSource.depRegistries)
	}

	multiSource.DedupAndTrackImplicitDependencies()
	return nil
}

func (multiSource *MultiSource) DedupAndTrackImplicitDependencies() {
	// Track implicit dependencies
	for _, dep := range multiSource.Dependencies {
		for i, join := range dep.Joins {
			depDataset := multiSource.DatasetManager.GetDataset(join.Dataset)
			if depDataset != nil && depDataset.IsProxy() {
				continue
			}
			if join.Dataset == multiSource.DatasetName {
				continue
			}
			// Track implicit dependency
			implicitDep := Dependency{}
			implicitDep.Dataset = join.Dataset
			implicitDep.Joins = dep.Joins[i+1:]
			multiSource.Dependencies = append(multiSource.Dependencies, implicitDep)
		}
	}

	// Dedup dependencies
	depMap := map[string]bool{}
	dedupedDependencies := []Dependency{}
	for _, dep := range multiSource.Dependencies {
		joinsKey := ""
		for _, join := range dep.Joins {
			joinsKey += join.Dataset + "|" + join.Predicate + "|" + strconv.FormatBool(join.Inverse)
		}
		depKey := dep.Dataset + ">" + joinsKey
		if _, ok := depMap[depKey]; !ok {
			dedupedDependencies = append(dedupedDependencies, dep)
		}
		depMap[depKey] = true
	}
	multiSource.Dependencies = dedupedDependencies
}

type DependencyRegistryJoin struct {
	ms        *MultiSource
	next      *DependencyRegistryJoin
	Dataset   string
	Predicate string
	Inverse   bool
}

// Hop implements DependencyRegistry
func (j *DependencyRegistryJoin) Hop(nextDataset string, predicate string) DependencyRegistry {
	j.next = &DependencyRegistryJoin{
		ms:        j.ms,
		next:      nil,
		Dataset:   nextDataset,
		Predicate: predicate,
		Inverse:   false,
	}
	return j.next
}

// IHop implements DependencyRegistry
func (j *DependencyRegistryJoin) IHop(nextDataset string, predicate string) DependencyRegistry {
	j.next = &DependencyRegistryJoin{
		ms:        j.ms,
		next:      nil,
		Dataset:   nextDataset,
		Predicate: predicate,
		Inverse:   true,
	}
	return j.next
}

func (multiSource *MultiSource) Hop(nextDataset string, predicate string) DependencyRegistry {
	tail := &DependencyRegistryJoin{
		ms:        multiSource,
		next:      nil,
		Dataset:   nextDataset,
		Predicate: predicate,
		Inverse:   false,
	}
	multiSource.depRegistries = append(multiSource.depRegistries, tail)
	return tail
}

func (multiSource *MultiSource) IHop(nextDataset string, predicate string) DependencyRegistry {
	tail := &DependencyRegistryJoin{
		ms:        multiSource,
		next:      nil,
		Dataset:   nextDataset,
		Predicate: predicate,
		Inverse:   true,
	}
	multiSource.depRegistries = append(multiSource.depRegistries, tail)
	return tail
}

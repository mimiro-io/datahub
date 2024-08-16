package server

import (
	"encoding/json"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"testing"
)

var _ = ginkgo.Describe("the IsEntityEqual function",
	func() {
		newEntities := func(f func(newEntity *Entity, prevEntity *Entity)) (*Entity, *Entity, []byte, []byte) {
			newEntity := NewEntity("ns0:entity1", 1)
			prevEntity := NewEntity("ns0:entity1", 1)
			newEntity.Properties["ns0:prop1"] = "value1"
			newEntity.Recorded = 5
			prevEntity.Properties["ns0:prop1"] = "value1"
			prevEntity.Recorded = 4

			// apply overrides
			if f != nil {
				f(newEntity, prevEntity)
			}

			newEntityJson, _ := json.Marshal(newEntity)
			prevEntityJson, _ := json.Marshal(prevEntity)
			json.Unmarshal(prevEntityJson, &prevEntity) // need to simulate that prevEntity is loaded from store and therefore unmarshaled

			return newEntity, prevEntity, newEntityJson, prevEntityJson
		}

		// mark as equal

		ginkgo.It("should return true: simple props", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(nil)
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeTrue())
		})
		ginkgo.It("should return true: number props", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = 1
				newEntity.Properties["ns0:prop1"] = 1
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeTrue())
		})
		ginkgo.It("should return true: float props", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = float32(1.5)
				newEntity.Properties["ns0:prop1"] = float32(1.5)
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeTrue())
		})
		ginkgo.It("should return true: bool props", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = true
				newEntity.Properties["ns0:prop1"] = true
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeTrue())
		})
		ginkgo.It("should return true: string array props", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = []string{"value1", "value2"}
				newEntity.Properties["ns0:prop1"] = []string{"value1", "value2"}
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeTrue())
		})
		ginkgo.It("should return true: any array props", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = []any{"value1", int64(1.0)}
				newEntity.Properties["ns0:prop1"] = []any{"value1", int64(1.0)}
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeTrue())
		})
		ginkgo.It("should return true: number array props", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = []int64{1, 2}
				newEntity.Properties["ns0:prop1"] = []int64{1, 2}
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeTrue())
		})
		ginkgo.It("should return true: bool array props", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = []bool{true, false}
				newEntity.Properties["ns0:prop1"] = []bool{true, false}
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeTrue())
		})
		ginkgo.It("should return true: nested entity props", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = NewEntity("ns0:entity2", 2)
				prevEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop2"] = "value2"
				prevEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop3"] = true
				prevEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop4"] = 1
				prevEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop5"] = []int64{1, 2}
				prevEntity.Properties["ns0:prop2"] = []*Entity{NewEntity("ns0:entity3", 3), NewEntity("ns0:entity4", 4)}

				newEntity.Properties["ns0:prop1"] = NewEntity("ns0:entity2", 2)
				newEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop2"] = "value2"
				newEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop3"] = true
				newEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop4"] = 1
				newEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop5"] = []int64{1, 2}
				newEntity.Properties["ns0:prop2"] = []*Entity{NewEntity("ns0:entity3", 3), NewEntity("ns0:entity4", 4)}
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeTrue())
		})

		// mark as different

		ginkgo.It("should return false: deleted", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				newEntity.IsDeleted = true
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
		})
		ginkgo.It("should return false:undeleted", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.IsDeleted = true
				newEntity.IsDeleted = false
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
		})
		ginkgo.It("should return false: add props", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				newEntity.Properties["ns0:prop1"] = "value2"
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
		})

		ginkgo.It("should return false: rm props", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = "value2"
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
		})
		ginkgo.It("should return false: change props", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = "value2"
				newEntity.Properties["ns0:prop1"] = "value3"
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())

			newEntity, prevEntity, newEntityJson, prevEntityJson = newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = 6.3
				newEntity.Properties["ns0:prop1"] = 6.4
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
		})
		ginkgo.It("should return false: different type", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = "1"
				newEntity.Properties["ns0:prop1"] = 1
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())

			newEntity, prevEntity, newEntityJson, prevEntityJson = newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = "hello"
				newEntity.Properties["ns0:prop1"] = []string{"hello"}
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())

			newEntity, prevEntity, newEntityJson, prevEntityJson = newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = []string{"hello"}
				newEntity.Properties["ns0:prop1"] = true
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
		})
		ginkgo.It("should return false: changed array", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = []int64{1, 2}
				newEntity.Properties["ns0:prop1"] = []int64{1, 3}
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
		})

		ginkgo.It("should return false: change in nested entity ", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.Properties["ns0:prop1"] = NewEntity("ns0:entity2", 2)
				prevEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop2"] = "value2"
				prevEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop3"] = true
				prevEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop4"] = 1
				prevEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop5"] = []int64{1, 2}

				newEntity.Properties["ns0:prop1"] = NewEntity("ns0:entity2", 2)
				newEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop2"] = "value2"
				newEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop3"] = true
				newEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop4"] = 1
				newEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop5"] = []int64{1, 3}
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
		})
		ginkgo.It("should return false: add ref", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				newEntity.References["ns0:ref1"] = "value1"
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
		})
		ginkgo.It("should return false: rm ref", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.References["ns0:ref1"] = "value1"
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
		})
		ginkgo.It("should return false: change ref", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.References["ns0:ref1"] = "value1"
				newEntity.References["ns0:ref1"] = "value2"
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
		})
		ginkgo.It("should return false: change ref array", func() {
			newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.References["ns0:ref1"] = []any{"value1", "value2"}
				newEntity.References["ns0:ref1"] = []any{"value1", "value3"}
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
			newEntity, prevEntity, newEntityJson, prevEntityJson = newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.References["ns0:ref1"] = "value1"
				newEntity.References["ns0:ref1"] = []any{"value1", "value3"}
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
			newEntity, prevEntity, newEntityJson, prevEntityJson = newEntities(func(newEntity *Entity, prevEntity *Entity) {
				prevEntity.References["ns0:ref1"] = []any{"value1", "value3"}
				newEntity.References["ns0:ref1"] = "value1"
			})
			Expect(IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)).To(BeFalse())
		})
	})

func BenchmarkIsEntityEqual(b *testing.B) {
	newEntities := func(f func(newEntity *Entity, prevEntity *Entity)) (*Entity, *Entity, []byte, []byte) {
		newEntity := NewEntity("ns0:entity1", 1)
		prevEntity := NewEntity("ns0:entity1", 1)
		newEntity.Properties["ns0:prop1"] = "value1"
		newEntity.Recorded = 5
		prevEntity.Properties["ns0:prop1"] = "value1"
		prevEntity.Recorded = 4

		// apply overrides
		if f != nil {
			f(newEntity, prevEntity)
		}

		newEntityJson, _ := json.Marshal(newEntity)
		prevEntityJson, _ := json.Marshal(prevEntity)
		json.Unmarshal(prevEntityJson, &prevEntity) // need to simulate that prevEntity is loaded from store and therefore unmarshaled

		return newEntity, prevEntity, newEntityJson, prevEntityJson
	}
	newEntity, prevEntity, newEntityJson, prevEntityJson := newEntities(func(newEntity *Entity, prevEntity *Entity) {
		prevEntity.Properties["ns0:prop1"] = NewEntity("ns0:entity2", 2)
		prevEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop2"] = "value2"
		prevEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop3"] = true
		prevEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop4"] = 1
		prevEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop5"] = []int64{1, 2}
		prevEntity.References["ns0:ref1"] = "value1"
		newEntity.Properties["ns0:prop1"] = NewEntity("ns0:entity2", 2)
		newEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop2"] = "value2"
		newEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop3"] = true
		newEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop4"] = 1
		newEntity.Properties["ns0:prop1"].(*Entity).Properties["ns0:prop5"] = []int64{1, 2}
		newEntity.References["ns0:ref1"] = "value1"
	})

	b.Run("IsEntityEqual with nested entities", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)
		}
	})

	b.Run("IsEntityEqualOld with nested entities", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			IsEntityEqualOld(prevEntityJson, newEntityJson, prevEntity, newEntity)
		}
	})
	newEntity, prevEntity, newEntityJson, prevEntityJson = newEntities(func(newEntity *Entity, prevEntity *Entity) {
		prevEntity.Properties["ns0:prop1"] = "value1"
		prevEntity.Properties["ns0:prop2"] = 10
		prevEntity.Properties["ns0:prop3"] = true
		prevEntity.Properties["ns0:prop4"] = 1.5
		prevEntity.References["ns0:ref1"] = "value1"
		prevEntity.References["ns0:ref2"] = "value2"
		prevEntity.References["ns0:ref3"] = "value3"
		prevEntity.References["ns0:ref4"] = "value4"

		newEntity.Properties["ns0:prop1"] = "value1"
		newEntity.Properties["ns0:prop2"] = 10
		newEntity.Properties["ns0:prop3"] = true
		newEntity.Properties["ns0:prop4"] = 1.5
		newEntity.References["ns0:ref1"] = "value1"
		newEntity.References["ns0:ref2"] = "value2"
		newEntity.References["ns0:ref3"] = "value3"
		newEntity.References["ns0:ref4"] = "value4"

	})
	b.Run("IsEntityEqual only literals", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)
		}
	})

	b.Run("IsEntityEqualOld only literals", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			IsEntityEqualOld(prevEntityJson, newEntityJson, prevEntity, newEntity)
		}
	})

	newEntity, prevEntity, newEntityJson, prevEntityJson = newEntities(func(newEntity *Entity, prevEntity *Entity) {
		prevEntity.Properties["ns0:prop1"] = []int64{1, 2}
		prevEntity.Properties["ns0:prop2"] = "hello"
		prevEntity.Properties["ns0:prop3"] = true
		prevEntity.Properties["ns0:prop4"] = 1.5
		prevEntity.References["ns0:ref1"] = "value1"
		prevEntity.References["ns0:ref2"] = []any{"value1", "value2"}
		newEntity.Properties["ns0:prop1"] = []int64{1, 2}
		newEntity.Properties["ns0:prop2"] = "hello"
		newEntity.Properties["ns0:prop3"] = true
		newEntity.Properties["ns0:prop4"] = 1.5
		newEntity.References["ns0:ref1"] = "value1"
		newEntity.References["ns0:ref2"] = []any{"value1", "value2"}
	})
	b.Run("IsEntityEqual literals and slices", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			IsEntityEqual(prevEntityJson, newEntityJson, prevEntity, newEntity)
		}
	})

	b.Run("IsEntityEqualOld literals and slices", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			IsEntityEqualOld(prevEntityJson, newEntityJson, prevEntity, newEntity)
		}
	})

}

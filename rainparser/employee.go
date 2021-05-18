package rainparser

// Employee schema
type Employee struct {
	info    map[string]string
	srcFile string
	isValid bool
}

// Regex for column names
var ColumnLabelRegex = map[string]string{
	"email":     `(?i)mail`,
	"name":      `(?i)^.?name|emp.*name`,
	"firstname": `(?i)f.*name|first`,
	"lastname":  `(?i)l.*name|last`,
	"salary":    `(?i)salary|wage|pay`,
	"phone":     `(?i)phn|phone|cont|mob`,
	"id":        `(?i)^id$|^num|emp.*id|emp.*num|user.*id`,
}

/**
Initialize values as 'nil' for columns not received and,
validate the employee info and,
update isValid flag for the employee record
*/
func (e *Employee) Standardise(finalColumnSet map[string]bool) {
	m := e.info
	for k := range finalColumnSet {
		_, ok := m[k]
		if !ok {
			m[k] = "nil"
		}
	}
	e.info = m
	validate(e)
}

// Validation Logic on emp data
func validate(e *Employee) bool {
	// todo: complete validation on all the fields
	return true
}

package operations

import (
	"reflect"
	"testing"
)

func TestGetStringSliceValue(t *testing.T) {
	tests := []struct {
		name     string
		args     map[string]string
		key      string
		expected []string
	}{
		{
			name:     "key not present returns nil",
			args:     map[string]string{},
			key:      "collections",
			expected: nil,
		},
		{
			name:     "empty brackets returns empty slice",
			args:     map[string]string{"collections": "[]"},
			key:      "collections",
			expected: []string{},
		},
		{
			name:     "single value with brackets",
			args:     map[string]string{"collections": "[col1]"},
			key:      "collections",
			expected: []string{"col1"},
		},
		{
			name:     "multiple values with brackets",
			args:     map[string]string{"collections": "[col1,col2,col3]"},
			key:      "collections",
			expected: []string{"col1", "col2", "col3"},
		},
		{
			name:     "values with spaces",
			args:     map[string]string{"collections": "[ col1 , col2 , col3 ]"},
			key:      "collections",
			expected: []string{"col1", "col2", "col3"},
		},
		{
			name:     "values without brackets",
			args:     map[string]string{"collections": "col1,col2,col3"},
			key:      "collections",
			expected: []string{"col1", "col2", "col3"},
		},
		{
			name:     "single value without brackets",
			args:     map[string]string{"collections": "col1"},
			key:      "collections",
			expected: []string{"col1"},
		},
		{
			name:     "empty string returns empty slice",
			args:     map[string]string{"collections": ""},
			key:      "collections",
			expected: []string{},
		},
		{
			name:     "whitespace only returns empty slice",
			args:     map[string]string{"collections": "   "},
			key:      "collections",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetStringSliceValue(tt.args, tt.key)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("GetStringSliceValue() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestGetInt64Value(t *testing.T) {
	tests := []struct {
		name     string
		args     map[string]string
		key      string
		def      int64
		expected int64
	}{
		{
			name:     "key not present returns default",
			args:     map[string]string{},
			key:      "size",
			def:      100,
			expected: 100,
		},
		{
			name:     "plain number",
			args:     map[string]string{"size": "500"},
			key:      "size",
			def:      100,
			expected: 500,
		},
		{
			name:     "kilobytes with K",
			args:     map[string]string{"size": "10K"},
			key:      "size",
			def:      0,
			expected: 10 * 1024,
		},
		{
			name:     "kilobytes with k lowercase",
			args:     map[string]string{"size": "10k"},
			key:      "size",
			def:      0,
			expected: 10 * 1024,
		},
		{
			name:     "megabytes with M",
			args:     map[string]string{"size": "100M"},
			key:      "size",
			def:      0,
			expected: 100 * 1024 * 1024,
		},
		{
			name:     "megabytes with m lowercase",
			args:     map[string]string{"size": "100m"},
			key:      "size",
			def:      0,
			expected: 100 * 1024 * 1024,
		},
		{
			name:     "gigabytes with G",
			args:     map[string]string{"size": "2G"},
			key:      "size",
			def:      0,
			expected: 2 * 1024 * 1024 * 1024,
		},
		{
			name:     "gigabytes with g lowercase",
			args:     map[string]string{"size": "2g"},
			key:      "size",
			def:      0,
			expected: 2 * 1024 * 1024 * 1024,
		},
		{
			name:     "terabytes with T",
			args:     map[string]string{"size": "1T"},
			key:      "size",
			def:      0,
			expected: 1 * 1024 * 1024 * 1024 * 1024,
		},
		{
			name:     "terabytes with t lowercase",
			args:     map[string]string{"size": "1t"},
			key:      "size",
			def:      0,
			expected: 1 * 1024 * 1024 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetInt64Value(tt.args, tt.key, tt.def)
			if result != tt.expected {
				t.Errorf("GetInt64Value() = %d, expected %d", result, tt.expected)
			}
		})
	}
}

func TestGetStringValue(t *testing.T) {
	tests := []struct {
		name     string
		args     map[string]string
		key      string
		def      string
		expected string
	}{
		{
			name:     "key not present returns default",
			args:     map[string]string{},
			key:      "database",
			def:      "_system",
			expected: "_system",
		},
		{
			name:     "key present returns value",
			args:     map[string]string{"database": "mydb"},
			key:      "database",
			def:      "_system",
			expected: "mydb",
		},
		{
			name:     "empty value returns empty string",
			args:     map[string]string{"database": ""},
			key:      "database",
			def:      "_system",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetStringValue(tt.args, tt.key, tt.def)
			if result != tt.expected {
				t.Errorf("GetStringValue() = %s, expected %s", result, tt.expected)
			}
		})
	}
}

func TestGetBoolValue(t *testing.T) {
	tests := []struct {
		name     string
		args     map[string]string
		key      string
		def      bool
		expected bool
	}{
		{
			name:     "key not present returns default false",
			args:     map[string]string{},
			key:      "drop",
			def:      false,
			expected: false,
		},
		{
			name:     "key not present returns default true",
			args:     map[string]string{},
			key:      "drop",
			def:      true,
			expected: true,
		},
		{
			name:     "true value",
			args:     map[string]string{"drop": "true"},
			key:      "drop",
			def:      false,
			expected: true,
		},
		{
			name:     "True value",
			args:     map[string]string{"drop": "True"},
			key:      "drop",
			def:      false,
			expected: true,
		},
		{
			name:     "1 value",
			args:     map[string]string{"drop": "1"},
			key:      "drop",
			def:      false,
			expected: true,
		},
		{
			name:     "yes value",
			args:     map[string]string{"drop": "yes"},
			key:      "drop",
			def:      false,
			expected: true,
		},
		{
			name:     "Yes value",
			args:     map[string]string{"drop": "Yes"},
			key:      "drop",
			def:      false,
			expected: true,
		},
		{
			name:     "false value",
			args:     map[string]string{"drop": "false"},
			key:      "drop",
			def:      true,
			expected: false,
		},
		{
			name:     "0 value",
			args:     map[string]string{"drop": "0"},
			key:      "drop",
			def:      true,
			expected: false,
		},
		{
			name:     "empty value",
			args:     map[string]string{"drop": ""},
			key:      "drop",
			def:      true,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetBoolValue(tt.args, tt.key, tt.def)
			if result != tt.expected {
				t.Errorf("GetBoolValue() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestParseArguments(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		expectedSubCmd string
		expectedMap    map[string]string
	}{
		{
			name:           "subcommand only",
			args:           []string{"insert"},
			expectedSubCmd: "insert",
			expectedMap:    map[string]string{},
		},
		{
			name:           "subcommand with key=value",
			args:           []string{"insert", "database=mydb", "collection=mycoll"},
			expectedSubCmd: "insert",
			expectedMap:    map[string]string{"database": "mydb", "collection": "mycoll"},
		},
		{
			name:           "subcommand with flag",
			args:           []string{"insert", "drop"},
			expectedSubCmd: "insert",
			expectedMap:    map[string]string{"drop": "true"},
		},
		{
			name:           "key=value with collections array",
			args:           []string{"insert", "database=mydb", "collections=[col1,col2,col3]"},
			expectedSubCmd: "insert",
			expectedMap:    map[string]string{"database": "mydb", "collections": "[col1,col2,col3]"},
		},
		{
			name:           "empty args",
			args:           []string{},
			expectedSubCmd: "",
			expectedMap:    map[string]string{},
		},
		{
			name:           "whitespace handling",
			args:           []string{"insert", "  database = mydb  "},
			expectedSubCmd: "insert",
			expectedMap:    map[string]string{"database": "mydb"},
		},
		{
			name:           "collections array with spaces - split across args",
			args:           []string{"insert", "database=mydb", "collections=[col1,", "col2,", "col3]"},
			expectedSubCmd: "insert",
			expectedMap:    map[string]string{"database": "mydb", "collections": "[col1,col2,col3]"},
		},
		{
			name:           "collections array with spaces inside single arg",
			args:           []string{"insert", "collections=[col1, col2, col3]"},
			expectedSubCmd: "insert",
			expectedMap:    map[string]string{"collections": "[col1, col2, col3]"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subCmd, m := ParseArguments(tt.args)
			if subCmd != tt.expectedSubCmd {
				t.Errorf("ParseArguments() subCmd = %s, expected %s", subCmd, tt.expectedSubCmd)
			}
			if !reflect.DeepEqual(m, tt.expectedMap) {
				t.Errorf("ParseArguments() map = %v, expected %v", m, tt.expectedMap)
			}
		})
	}
}

func TestNewNormalProgSizeValidation(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "insert with size only - valid",
			args:        []string{"insert", "database=mydb", "collection=mycoll", "size=100M"},
			expectError: false,
		},
		{
			name:        "insert with sizePerCollection and collections - valid",
			args:        []string{"insert", "database=mydb", "collections=[col1,col2]", "sizePerCollection=100M"},
			expectError: false,
		},
		{
			name:        "insert with both size and sizePerCollection - invalid",
			args:        []string{"insert", "database=mydb", "collections=[col1,col2]", "size=100M", "sizePerCollection=50M"},
			expectError: true,
			errorMsg:    "size and sizePerCollection are mutually exclusive",
		},
		{
			name:        "insert with sizePerCollection but no collections - invalid",
			args:        []string{"insert", "database=mydb", "collection=mycoll", "sizePerCollection=100M"},
			expectError: true,
			errorMsg:    "sizePerCollection requires collections to be specified",
		},
		{
			name:        "insert with collections but no sizePerCollection - invalid",
			args:        []string{"insert", "database=mydb", "collections=[col1,col2]"},
			expectError: true,
			errorMsg:    "when using collections, sizePerCollection must be specified",
		},
		{
			name:        "create subcommand ignores size validation",
			args:        []string{"create", "database=mydb", "collection=mycoll"},
			expectError: false,
		},
		{
			name:        "randomRead subcommand ignores size validation",
			args:        []string{"randomRead", "database=mydb", "collection=mycoll"},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewNormalProg(tt.args, 1)
			if tt.expectError {
				if err == nil {
					t.Errorf("NewNormalProg() expected error containing %q, got nil", tt.errorMsg)
				} else if tt.errorMsg != "" && !contains(err.Error(), tt.errorMsg) {
					t.Errorf("NewNormalProg() error = %q, expected to contain %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("NewNormalProg() unexpected error: %v", err)
				}
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && stringContains(s, substr)))
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}


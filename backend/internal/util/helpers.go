package util

import "go.bryk.io/pkg/ulid"

func RemoveUlid(slice []ulid.ULID, u ulid.ULID) []ulid.ULID {
	result := make([]ulid.ULID, 0, len(slice))

	for _, item := range slice {
		if item != u {
			result = append(result, item)
		}
	}
	return result
}

func RemoveStr(slice []string, u string) []string {
	result := make([]string, 0, len(slice))

	for _, item := range slice {
		if item != u {
			result = append(result, item)
		}
	}
	return result
}

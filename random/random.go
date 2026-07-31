package random

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
)

const (
	alphabet = "abcdefghijklmnopqrstuvwxyz"
)

func RandomInt(min, max int64) int64 {
	return min + rand.Int63n(max-min+1)
}
func RandomNullFloat(min, max int64) *float64 {
	result := float64(min + rand.Int63n(max-min+1))
	return &result

}
func RandomNullInt(min, max int64) *int32 {
	result := int32(min + rand.Int63n(max-min+1))
	return &result
}

func RandomString(n int) string {
	var sb strings.Builder
	k := len(alphabet)
	for range n {
		c := alphabet[rand.Intn(k)]
		sb.WriteByte(c)

	}
	return sb.String()
}

func RandomNullString(n int) *string {
	var sb strings.Builder
	k := len(alphabet)
	for range n {
		c := alphabet[rand.Intn(k)]
		sb.WriteByte(c)

	}
	result := sb.String()
	return &result
}

func RandomUrl() string {
	return fmt.Sprintf("https://%v.com", RandomString(5))
}
func RandomNullUrl() *string {
	result := fmt.Sprintf("https://%v.com", RandomString(5))
	return &result
}

func RandomPhoneNumber() string {
	var numbers []string
	var res string
	for range 12 {
		numbers = append(numbers, fmt.Sprint(RandomInt(0, 9)))
	}
	res = strings.Join(numbers, "")
	return fmt.Sprintf("+%v", res)
}
func RandomPhoneNumbers() []string {
	phonNumbers := []string{}
	for range 5 {
		phonNumbers = append(phonNumbers, RandomPhoneNumber())
	}
	return phonNumbers
}

func RandomColor() string {
	r := rand.Intn(180) + 75
	g := rand.Intn(180) + 75
	b := rand.Intn(180) + 75

	hexColor := fmt.Sprintf("#%02X%02X%02X", r, g, b)

	return hexColor
}
func RandomNullColor() *string {
	r := rand.Intn(180) + 75
	g := rand.Intn(180) + 75
	b := rand.Intn(180) + 75

	hexColor := fmt.Sprintf("#%02X%02X%02X", r, g, b)

	return &hexColor
}
func RandomEmail() string {
	return fmt.Sprintf("%v@gmail.com", RandomString(6))
}
func RandomNullEmail() *string {
	result := fmt.Sprintf("%v@gmail.com", RandomString(6))
	return &result
}

func RandomKeyWords() []string {
	keyWords := []string{}
	for range 5 {
		keyWords = append(keyWords, RandomString(5))
	}
	return keyWords
}

func RandomBool() bool {
	random := RandomInt(0, 1)
	boolean := random != 0
	return boolean

}

func RandomRating() int32 {
	return int32(RandomInt(0, 5))
}

func RandomReply() json.RawMessage {
	randomUsername := RandomString(5)
	reply := map[string]string{
		"username": randomUsername,
	}
	jsonData, err := json.Marshal(reply)
	if err != nil {
		return nil
	}
	return json.RawMessage(jsonData)
}

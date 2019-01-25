package utils

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type ParseError struct {
	reason string
}

func (e *ParseError) Error() string {
	return e.reason
}

func HandleError(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func ParseFilterExpresion(filterExp string) ([]string, error) {
	if filterExp == "" {
		return nil, &ParseError{"filter expresion is empty"}
	}

	vbids := make([]string, 0)
	for _, vbid := range strings.Split(filterExp, ",") {
		_, err := strconv.Atoi(vbid)
		if err != nil {
			if rng := strings.Split(vbid, "-"); len(rng) == 2 {
				start, err := strconv.Atoi(rng[0])
				if err != nil {
					return nil, &ParseError{"invalid range start: " + rng[0]}
				}
				end, err := strconv.Atoi(rng[1])
				if err != nil {
					return nil, &ParseError{"invalid range end: " + rng[1]}
				}

				if start >= end {
					return nil, &ParseError{"range start must be smaller than range end"}
				}

				for vbid := start; vbid <= end; vbid++ {
					vbids = append(vbids, strconv.Itoa(vbid))
				}

				continue
			}
			return nil, &ParseError{vbid + " is not a valid vbucket"}
		}

		vbids = append(vbids, vbid)
	}

	return vbids, nil
}

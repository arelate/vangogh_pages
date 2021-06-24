package vangogh_pages

import (
	"encoding/json"
	"fmt"
	"github.com/arelate/gog_media"
	"github.com/arelate/gog_types"
	"github.com/arelate/vangogh_products"
	"github.com/arelate/vangogh_urls"
	"github.com/boggydigital/kvas"
	"io"
	"log"
	"net/http"
	"runtime"
	"strconv"
)

type pageReadCloser struct {
	page       string
	readCloser io.ReadCloser
}

func GetAllPages(
	httpClient *http.Client,
	pt vangogh_products.ProductType,
	mt gog_media.Media) error {

	errors := make(chan error)
	pageReadClosers := make(chan *pageReadCloser)

	defer close(pageReadClosers)
	defer close(errors)

	localDir, err := vangogh_urls.LocalProductsDir(pt, mt)
	if err != nil {
		return err
	}

	valueSet, err := kvas.NewJsonLocal(localDir)
	if err != nil {
		return err
	}

	concurrentPages := 1
	if err := getPages(httpClient, valueSet, 0, 1, concurrentPages, pt, mt, pageReadClosers, errors); err != nil {
		return err
	}

	// read back the value, decode and extract totalPages
	readCloser, err := valueSet.Get("1")
	if err != nil {
		return err
	}
	var page gog_types.TotalPages
	if err = json.NewDecoder(readCloser).Decode(&page); err != nil {
		return err
	}
	totalPages := page.TotalPages

	concurrentPages = runtime.NumCPU()
	return getPages(httpClient, valueSet, 1, totalPages, concurrentPages, pt, mt, pageReadClosers, errors)
}

func getPages(
	httpClient *http.Client,
	valueSet *kvas.ValueSet,
	page, totalPages, concurrentPages int,
	pt vangogh_products.ProductType,
	mt gog_media.Media,
	pageReadClosers chan *pageReadCloser,
	errors chan error) error {

	remainingPages := totalPages - page
	for remainingPages > 0 {

		for i := 0; i < concurrentPages; i++ {
			page++
			if page > totalPages {
				break
			}
			fmt.Printf("getting %s (%s) page %d\n", pt, mt, page)
			go getPage(httpClient, strconv.Itoa(page), pt, mt, pageReadClosers, errors)
		}
		concurrentPages = 0

		select {
		case err := <-errors:
			log.Println(err)
			return err
		case pageRC := <-pageReadClosers:
			// TODO: make this serial to keep kvas unchanged until rewrite
			if err := valueSet.Set(pageRC.page, pageRC.readCloser); err != nil {
				return err
			}
			pageRC.readCloser.Close()
			remainingPages--
			concurrentPages++
		}
	}
	return nil
}

func getPage(
	httpClient *http.Client,
	page string,
	pt vangogh_products.ProductType,
	mt gog_media.Media,
	pageReadClosers chan *pageReadCloser,
	errors chan error) {

	remoteUrl, err := vangogh_urls.RemoteProductsUrl(pt)
	if err != nil {
		errors <- err
		return
	}

	pageUrl := remoteUrl(page, mt)

	resp, err := httpClient.Get(pageUrl.String())
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		errors <- err
		return
	}

	pageReadClosers <- &pageReadCloser{
		page:       page,
		readCloser: resp.Body,
	}
}

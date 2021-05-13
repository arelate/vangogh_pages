module github.com/arelate/vangogh_pages

go 1.16

require (
	github.com/arelate/gog_media v0.1.3-alpha
	github.com/arelate/gog_types v0.1.8-alpha
	github.com/arelate/vangogh_products v0.1.3-alpha
	github.com/arelate/vangogh_urls v0.1.0-alpha
	github.com/boggydigital/kvas v0.1.9-alpha
)

replace (
	github.com/arelate/gog_media => ../gog_media
	github.com/arelate/gog_types => ../gog_types
	github.com/arelate/gog_urls => ../gog_urls
	github.com/arelate/vangogh_images => ../vangogh_images
	github.com/arelate/vangogh_products => ../vangogh_products
	github.com/arelate/vangogh_urls => ../vangogh_urls
	github.com/boggydigital/kvas => ../kvas
)

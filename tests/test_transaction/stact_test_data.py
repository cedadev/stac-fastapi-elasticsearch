test_collection = {
    "type": "collection",
    "id": "pytest_collection",
    "stac_version": "1.0.0",
    "stac_extensions": [

    ],
    "title": "TEST",
    "description": "testing collection",
    "keywords": [

    ],
    "extent": {
        "temporal": {
            "gte": "2000-02-23T23:55:00"
        },
        "spatial": {
            "type": "envelope",
            "coordinates": [
                [
                    -180,
                    90
                ],
                [
                    180,
                    -90
                ]
            ]
        }
    }
}

test_item = {
    "type": "item",
    "id": "pytest_item",
    "stac_version": "1.0.0",
    "collection": "pytest_collection",
    "properties": {
        "platform": "test_platform",
        "product_version": "v1.0.0",
        "processing_level": "L2",
        "variable": "Test",
        "start_datetime": "2020-11-29T05:13:00",
        "end_datetime": "2020-11-29T06:54:30",
        "orbit": "16214",
        "datetime": "2020-12-02T19:56:40",
        "institution": "TEST/INST",
        "sensor": "TESTSENSOR"
    },
    "assets": {
        "test_asset_1": {
            "href": "http://data.ceda.ac.uk/neodc/sentinel5p/data/L2_CH4/v1.4/2020/11/29/S5P_OFFL_L2__CH4____20201129T051300_20201129T065430_16214_01_010400_20201202T195640.nc",
            "type": "application/x-hdf",
            "title": "S5P_OFFL_L2__CH4____20201129T051300_20201129T065430_16214_01_010400_20201202T195640.nc",
            "roles": [
                "data"
            ]
        },
        "test_asset_2": {
            "href": "http://data.ceda.ac.uk/neodc/sentinel5p/data/L2_CH4/v1.4/2020/11/29/S5P_OFFL_L2__CH4____20201129T051300_20201129T065430_16214_01_010400_20201202T195640_checksum",
            "type": "text/plain",
            "title": "S5P_OFFL_L2__CH4____20201129T051300_20201129T065430_16214_01_010400_20201202T195640_checksum",
            "roles": [
                "data"
            ]
        }
    }
}

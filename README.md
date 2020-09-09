# Test project to use pyspark to transform a nested JSON file into a flat CSV file


## Input

This test project uses the following JSON input data (test-input.json):

```

{
	"id": "1",
	"extendedAttributes": 
	[
		{
			"id": "2",
			"attributeId": "3",
			"text": "KNO315189A",
			"dropDownListItem": {
				"id": "4",
				"customId": "DLDQC"		
			}
		},
		{
			"id": "5",
			"attributeId": "6",
			"text": "NWISMD",
			"dropDownListItem": {
				"id": "7",
				"customId": "< - less than"		
			}
		}
	]
}

```


## Output

The goal is to produce CSV data in the following format:

```

id,extendedAttributeIds,extendedAttributeAttributeIds,extendedAttributeTexts,extendedAttributeListItemIds,extendedAttributeListItemCustomIds
1, "2,5", "3,6", "KNO315189A,NWISMD", "4,7", "DLDQC,< - less than"

```

## Install

Just install the pyspark package:

1. pip3 install pyspark


## Run

Execute the following command:

* python3 test-transform.py


## Verify

Check the output in the folder output.csv

* For example, cat output.csv/*.csv

{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/1.0.0/schema.json",
  "name": "executive-dashboard",
  "displayName": "Executive Dashboard",
  "displayOption": "FitToPage",
  "width": 1280,
  "height": 720,
  "visualContainers": [
    {
      "position": { "x": 20, "y": 20, "width": 200, "height": 100, "z": 0 },
      "visual": {
        "visualType": "card",
        "objects": {
          "labels": [{ "properties": { "fontSize": { "expr": { "Literal": { "Value": "28D" } } } } }]
        },
        "prototypeQuery": {
          "Select": [
            { "Measure": { "Expression": { "SourceRef": { "Entity": "{{PRIMARY_FACT_TABLE}}" } }, "Property": "{{KPI_1_MEASURE}}" }, "Name": "KPI1" }
          ]
        }
      }
    },
    {
      "position": { "x": 240, "y": 20, "width": 200, "height": 100, "z": 0 },
      "visual": {
        "visualType": "card",
        "prototypeQuery": {
          "Select": [
            { "Measure": { "Expression": { "SourceRef": { "Entity": "{{PRIMARY_FACT_TABLE}}" } }, "Property": "{{KPI_2_MEASURE}}" }, "Name": "KPI2" }
          ]
        }
      }
    },
    {
      "position": { "x": 460, "y": 20, "width": 200, "height": 100, "z": 0 },
      "visual": {
        "visualType": "card",
        "prototypeQuery": {
          "Select": [
            { "Measure": { "Expression": { "SourceRef": { "Entity": "{{PRIMARY_FACT_TABLE}}" } }, "Property": "{{KPI_3_MEASURE}}" }, "Name": "KPI3" }
          ]
        }
      }
    },
    {
      "position": { "x": 680, "y": 20, "width": 200, "height": 100, "z": 0 },
      "visual": {
        "visualType": "card",
        "prototypeQuery": {
          "Select": [
            { "Measure": { "Expression": { "SourceRef": { "Entity": "{{PRIMARY_FACT_TABLE}}" } }, "Property": "{{KPI_4_MEASURE}}" }, "Name": "KPI4" }
          ]
        }
      }
    },
    {
      "position": { "x": 20, "y": 140, "width": 600, "height": 280, "z": 0 },
      "visual": {
        "visualType": "lineChart",
        "prototypeQuery": {
          "Select": [
            { "Column": { "Expression": { "SourceRef": { "Entity": "DimDate" } }, "Property": "Date" }, "Name": "Axis" },
            { "Measure": { "Expression": { "SourceRef": { "Entity": "{{PRIMARY_FACT_TABLE}}" } }, "Property": "{{KPI_1_MEASURE}}" }, "Name": "Values" }
          ]
        }
      }
    },
    {
      "position": { "x": 640, "y": 140, "width": 300, "height": 280, "z": 0 },
      "visual": {
        "visualType": "donutChart",
        "prototypeQuery": {
          "Select": [
            { "Column": { "Expression": { "SourceRef": { "Entity": "{{CATEGORY_TABLE}}" } }, "Property": "{{CATEGORY_COLUMN}}" }, "Name": "Category" },
            { "Measure": { "Expression": { "SourceRef": { "Entity": "{{PRIMARY_FACT_TABLE}}" } }, "Property": "{{KPI_1_MEASURE}}" }, "Name": "Values" }
          ]
        }
      }
    },
    {
      "position": { "x": 20, "y": 440, "width": 920, "height": 260, "z": 0 },
      "visual": {
        "visualType": "tableEx",
        "prototypeQuery": {
          "Select": [
            { "Column": { "Expression": { "SourceRef": { "Entity": "{{DETAIL_TABLE}}" } }, "Property": "{{DETAIL_COLUMN}}" }, "Name": "Detail" },
            { "Measure": { "Expression": { "SourceRef": { "Entity": "{{PRIMARY_FACT_TABLE}}" } }, "Property": "{{KPI_1_MEASURE}}" }, "Name": "M1" },
            { "Measure": { "Expression": { "SourceRef": { "Entity": "{{PRIMARY_FACT_TABLE}}" } }, "Property": "{{KPI_2_MEASURE}}" }, "Name": "M2" }
          ]
        }
      }
    }
  ]
}

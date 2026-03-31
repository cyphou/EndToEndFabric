	measure '{{MEASURE_NAME}}' =
		{{DAX_EXPRESSION}}
		formatString: {{FORMAT_STRING}}
{{#IF DISPLAY_FOLDER}}
		displayFolder: {{DISPLAY_FOLDER}}
{{/IF}}
{{#IF DESCRIPTION}}
		description: {{DESCRIPTION}}
{{/IF}}

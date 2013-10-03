<schema xmlns="http://purl.oclc.org/dsdl/schematron">
	<title>Schematron for checking Jpylyzer outputs</title> 
	<pattern>
		<title>validity test</title>
		<rule context="jpylyzer/isValidJP2">
			<assert test="text()='True'">JP2 must be valid</assert>
		</rule>
	</pattern>
	<pattern>
		<title>profile test</title>
		<rule context="cod/order">
			<assert test="text()='RPCL'">progression order must fit profile</assert>
		</rule>
		<rule context="cod/transformation">
			<assert test="text()='9-7 irreversible'">transformation must fit profile</assert>
		</rule>
		<rule context="cod/precincts">
			<assert test="text()='yes'">must have precincts to fit profile</assert>
		</rule>
		<rule context="cod">
			<assert test="count(precinctSizeX)=7">number of precincts must fit profile</assert>
		</rule>
		<rule context="siz/numberOfTiles">
			<assert test="text()='1'">number of tiles must fit profile</assert>
		</rule>
		<rule context="cod/levels">
			<assert test="text()='6'">number of levels must fit profile</assert>
		</rule>
		<rule context="cod/layers">
			<assert test="text()='12'">number of layers must fit profile</assert>
		</rule>
		<rule context="cod/codingBypass">
			<assert test="text()='no'">must have coding bypass to fit profile</assert>
		</rule>
		<rule context="cod/codeBlockWidth">
			<assert test="text()='64'">number of precincts must fit profile</assert>
		</rule>
		<rule context="cod/codeBlockHeight">
			<assert test="text()='64'">number of precincts must fit profile</assert>
		</rule>
		
	</pattern>
</schema>


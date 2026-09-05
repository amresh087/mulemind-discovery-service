package com.mulemind.discovery;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.mulemind.discovery.service.ProjectArchiveProcessor;

@SpringBootTest
class ReportingServiceApplicationTests {

	@Test
	void contextLoads() {
	}

	@Test
	void extractsExplicitDataWeaveRequestFieldsWhenNoContractExists() throws Exception {
		ProjectArchiveProcessor processor = new ProjectArchiveProcessor();
		Path destination = Files.createTempDirectory("discovery-dw-");

		String dataWeave = "output application/json --- { id: payload.id, name: payload.name, city: payload.city }";
		assertEquals(List.of("id", "name", "city"),
				processor.parseArchive(zip("src/main/mule/flow.dwl", dataWeave), destination).getRequestFields());
	}

	@Test
	void extractsRequestBodyFieldsFromRamlAndIgnoresResponseType() throws Exception {
		ProjectArchiveProcessor processor = new ProjectArchiveProcessor();
		Path destination = Files.createTempDirectory("discovery-raml-");

		String raml = "#%RAML 1.0\ntypes:\n  UserRequest:\n    type: object\n    properties:\n      id: integer\n      name: string\n      city?: string\n  UserResponse:\n    type: object\n    properties:\n      message: string\n      user: UserRequest\n/users:\n  post:\n    body:\n      application/json:\n        type: UserRequest\n";
		assertEquals(List.of("id", "name", "city"),
				processor.parseArchive(zip("api.raml", raml), destination).getRequestFields());
	}

	@Test
	void returnsNoFieldsWithoutContractOrExplicitPayloadAccess() throws Exception {
		ProjectArchiveProcessor processor = new ProjectArchiveProcessor();
		Path destination = Files.createTempDirectory("discovery-empty-");

		assertEquals(List.of(), processor.parseArchive(zip("flow.dwl", "output application/json --- { value: vars.value }"), destination)
				.getRequestFields());
	}

	@Test
	void contractFieldsOverrideDataWeaveFallback() throws Exception {
		ProjectArchiveProcessor processor = new ProjectArchiveProcessor();
		Path destination = Files.createTempDirectory("discovery-precedence-");
		String raml = "#%RAML 1.0\ntypes:\n  UserRequest:\n    type: object\n    properties:\n      id: integer\n/users:\n  post:\n    body:\n      application/json:\n        type: UserRequest\n";

		assertEquals(List.of("id"), processor.parseArchive(zip(Map.of(
				"api.raml", raml,
				"flow.dwl", "output application/json --- { id: payload.id, name: payload.name }")), destination)
				.getRequestFields());
	}

	@Test
	void extractsInlineRamlRequestBodyFields() throws Exception {
		ProjectArchiveProcessor processor = new ProjectArchiveProcessor();
		Path destination = Files.createTempDirectory("discovery-inline-raml-");
		String raml = "#%RAML 1.0\n/users:\n  post:\n    body:\n      application/json:\n        properties:\n          id: integer\n          name: string\n";

		assertEquals(List.of("id", "name"), processor.parseArchive(zip("api.raml", raml), destination).getRequestFields());
	}

	@Test
	void extractsDataWeaveAccessFromMuleXmlTransform() throws Exception {
		ProjectArchiveProcessor processor = new ProjectArchiveProcessor();
		Path destination = Files.createTempDirectory("discovery-xml-dw-");
		String xml = "<mule><flow name=\"users\"><ee:transform><ee:set-payload><![CDATA[output application/json --- { id: payload.id }]]></ee:set-payload></ee:transform></flow></mule>";

		assertEquals(List.of("id"), processor.parseArchive(zip("src/main/mule/flow.xml", xml), destination).getRequestFields());
	}

	private static byte[] zip(String fileName, String content) throws Exception {
		return zip(Map.of(fileName, content));
	}

	private static byte[] zip(Map<String, String> files) throws Exception {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try (ZipOutputStream zip = new ZipOutputStream(output)) {
			for (Map.Entry<String, String> file : files.entrySet()) {
				zip.putNextEntry(new ZipEntry(file.getKey()));
				zip.write(file.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8));
				zip.closeEntry();
			}
		}
		return output.toByteArray();
	}

}

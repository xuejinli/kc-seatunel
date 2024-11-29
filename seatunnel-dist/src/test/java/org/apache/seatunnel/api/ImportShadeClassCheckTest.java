/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.Range;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.NodeList;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.READ;

@Slf4j
public class ImportShadeClassCheckTest {

    private static Map<String, NodeList<ImportDeclaration>> importsMap = new HashMap<>();
    private final String SEATUNNEL_SHADE_PREFIX = "org.apache.seatunnel.shade.";
    public static final boolean isWindows =
            System.getProperty("os.name").toLowerCase().startsWith("win");
    private static final String JAVA_FILE_EXTENSION = ".java";
    private static final JavaParser JAVA_PARSER = new JavaParser();

    @BeforeAll
    public static void beforeAll() {
        Path directoryPath = Paths.get(System.getProperty("user.dir")).getParent();
        log.info("work directory parent ===>  " + directoryPath);
        try {
            Files.walk(directoryPath)
                    .filter(path -> path.toString().endsWith(JAVA_FILE_EXTENSION))
                    .forEach(
                            path -> {
                                try (InputStream inputStream = Files.newInputStream(path, READ)) {
                                    ParseResult<CompilationUnit> parseResult =
                                            JAVA_PARSER.parse(inputStream);
                                    Optional<CompilationUnit> result = parseResult.getResult();
                                    if (result.isPresent()) {
                                        importsMap.put(path.toString(), result.get().getImports());
                                    } else {
                                        log.error("Failed to parse Java file: " + path);
                                    }
                                } catch (IOException e) {
                                    log.error(
                                            "IOException occurred while processing file: " + path,
                                            e);
                                }
                            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to walk through directory", e);
        }
    }

    @Test
    public void guavaShadeCheck() {
        Map<String, List<String>> errorMap =
                checkShade(Collections.singletonList("com.google.common"));
        Assertions.assertEquals(0, errorMap.size(), errorMsg("guava", errorMap));
        log.info("check guava shade successfully");
    }

    @Test
    public void jacksonShadeCheck() {
        Map<String, List<String>> errorMap =
                checkShade(
                        Collections.singletonList("com.fasterxml.jackson"),
                        Arrays.asList(
                                "org.apache.seatunnel.format.compatible.debezium.json",
                                "org.apache.seatunnel.format.compatible.kafka.connect.json",
                                "org.apache.seatunnel.connectors.druid.sink",
                                "org.apache.seatunnel.connectors.seatunnel.typesense.client"));
        Assertions.assertEquals(0, errorMap.size(), errorMsg("jackson", errorMap));
        log.info("check jackson shade successfully");
    }

    @Test
    public void jettyShadeCheck() {
        Map<String, List<String>> errorMap =
                checkShade(Collections.singletonList("org.eclipse.jetty"));
        Assertions.assertEquals(0, errorMap.size(), errorMsg("jetty", errorMap));
        log.info("check jetty shade successfully");
    }

    @Test
    public void janinoShadeCheck() {
        Map<String, List<String>> errorMap =
                checkShade(Arrays.asList("org.codehaus.janino", "org.codehaus.commons"));
        Assertions.assertEquals(0, errorMap.size(), errorMsg("janino", errorMap));
        log.info("check janino shade successfully");
    }

    private Map<String, List<String>> checkShade(List<String> prefixList) {
        return checkShade(prefixList, Collections.emptyList());
    }

    private Map<String, List<String>> checkShade(
            List<String> prefixList, List<String> packageWhiteList) {
        Map<String, List<String>> errorMap = new HashMap<>();
        importsMap.forEach(
                (clazzPath, imports) -> {
                    boolean match =
                            packageWhiteList.stream()
                                    .map(
                                            whitePackage ->
                                                    whitePackage.replace(
                                                            ".", isWindows ? "\\" : "/"))
                                    .anyMatch(clazzPath::contains);
                    if (!match) {
                        List<String> collect =
                                imports.stream()
                                        .filter(
                                                importDeclaration -> {
                                                    String importClz =
                                                            importDeclaration.getName().asString();
                                                    return prefixList.stream()
                                                            .anyMatch(importClz::startsWith);
                                                })
                                        .map(this::getImportClassLineNum)
                                        .collect(Collectors.toList());
                        if (!collect.isEmpty()) {
                            errorMap.put(clazzPath, collect);
                        }
                    }
                });
        return errorMap;
    }

    private String errorMsg(String checkType, Map<String, List<String>> errorMap) {
        StringBuilder msg = new StringBuilder();
        msg.append(String.format("%s shade is not up to code, need add prefix [", checkType))
                .append(SEATUNNEL_SHADE_PREFIX)
                .append("]. \n");
        errorMap.forEach(
                (key, value) -> {
                    msg.append(key).append("\n");
                    value.forEach(lineNum -> msg.append(lineNum).append("\n"));
                });
        return msg.toString();
    }

    private String getImportClassLineNum(ImportDeclaration importDeclaration) {
        Range range = importDeclaration.getRange().get();
        return String.format("%s  [%s]", importDeclaration.getName().asString(), range.end.line);
    }
}

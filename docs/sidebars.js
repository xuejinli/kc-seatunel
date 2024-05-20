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

/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
    // By default, Docusaurus generates a sidebar from the docs folder structure
    // tutorialSidebar: [{type: 'autogenerated', dirName: '.'}],

    // But you can create a sidebar manually
    /*
    tutorialSidebar: [
      {
        type: 'category',
        label: 'Tutorial',
        items: ['hello'],
      },
    ],
     */

    "docs": [
        "about",
        {
            "type": "category",
            "label": "Quick Start - V2",
            "items": [
                {
                    "type": "category",
                    "label": "Start With Locally",
                    "items": [
                        {
                            "type": "autogenerated",
                            "dirName": "start-v2/locally"
                        }
                    ]
                },
                {
                    "type": "category",
                    "label": "Start With Docker",
                    "items": [
                        {
                            "type": "autogenerated",
                            "dirName": "start-v2/docker"
                        }
                    ]
                },
                {
                    "type": "category",
                    "label": "Start With K8s",
                    "items": [
                        {
                            "type": "autogenerated",
                            "dirName": "start-v2/kubernetes"
                        }
                    ]
                }
            ]
        },
        {
            "type": "category",
            "label": "Concept",
            "items": [
                "concept/config",
                "concept/connector-v2-features",
                'concept/schema-feature',
                'concept/JobEnvConfig',
                'concept/sql-config',
                'concept/speed-limit',
                'concept/event-listener'
            ]
        },
        "Connector-v2-release-state",
        {
            "type": "category",
            "label": "Connector-V2",
            "items": [
                {
                    "type": "category",
                    "label": "Source",
                    "link": {
                        "type": "generated-index",
                        "title": "Source(V2) of SeaTunnel",
                        "description": "List all source(v2) supported Apache SeaTunnel for now.",
                        "slug": "/category/source-v2",
                        "keywords": ["source"],
                        "image": "/img/favicon.ico"
                    },
                    "items": [
                        {
                            "type": "autogenerated",
                            "dirName": "connector-v2/source"
                        }
                    ]
                },
                {
                    "type": "category",
                    "label": "Sink",
                    "link": {
                        "type": "generated-index",
                        "title": "Sink(V2) of SeaTunnel",
                        "description": "List all sink(v2) supported Apache SeaTunnel for now.",
                        "slug": "/category/sink-v2",
                        "keywords": ["sink"],
                        "image": "/img/favicon.ico"
                    },
                    "items": [
                        {
                            "type": "autogenerated",
                            "dirName": "connector-v2/sink"
                        }
                    ]
                },
                "connector-v2/Error-Quick-Reference-Manual",
                "connector-v2/Config-Encryption-Decryption"
            ]
        },
        {
            "type": "category",
            "label": "Transform-V2",
            "link": {
                "type": "generated-index",
                "title": "Transform V2 of SeaTunnel",
                "description": "List all transform v2 supported Apache SeaTunnel for now.",
                "slug": "/category/transform-v2",
                "keywords": ["transform-v2"],
                "image": "/img/favicon.ico"
            },
            "items": [
                {
                    "type": "autogenerated",
                    "dirName": "transform-v2"
                }
            ]
        },
        {
            "type": "category",
            "label": "Command",
            "items": [
                "command/usage",
                "command/connector-check"
            ]
        },
        {
            "type": "category",
            "label": "SeaTunnel Engine",
            "items": [
                "seatunnel-engine/about",
                "seatunnel-engine/deployment",
                "seatunnel-engine/local-mode",
                "seatunnel-engine/cluster-mode",
                "seatunnel-engine/savepoint",
                "seatunnel-engine/checkpoint-storage",
                "seatunnel-engine/rest-api",
                "seatunnel-engine/tcp",
                "seatunnel-engine/engine-jar-storage-mode"
            ]
        },
        {
            "type": "category",
            "label": "Other Engine",
            "items": [
                "other-engine/flink",
                "other-engine/spark"
            ]
        },
        {
            type: 'category',
            label: 'Contribution',
            items: [
                'contribution/setup',
                'contribution/new-license',
                'contribution/coding-guide',
                'contribution/contribute-transform-v2-guide',
            ],
        },
        "faq"
    ]
};

module.exports = sidebars

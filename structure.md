.
├── api
│   ├── auth_middleware.py
│   ├── inbound_api.py
│   └── __init__.py
├── config
│   ├── __init__.py
│   ├── init.sql
│   └── settings.py
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── extractors
│   ├── base_extractor.py
│   ├── db_extractor.py
│   ├── file_extractor.py
│   ├── http_extractor.py
│   ├── __init__.py
│   └── open_meteo_extractor.py
├── .gitignore
├── knowledge_base
│   ├── Architecture.md
│   ├── GetFolderStructureAsDiagram.md 
│   ├── project1_folder_structure.svg
│   ├── Project-Summary.md
│   ├── SourceIdentification.md
│   ├── Story.md
│   └── TrenchWar.md
├── LICENSE
├── loaders
│   ├── __init__.py
│   └── postgres_loader.py
├── logs
│   └── .gitkeep
├── outbound
│   ├── edi_generator.py
│   ├── event_router.py
│   ├── http_caller.py
│   ├── __init__.py
│   ├── sftp_dropper.py
│   └── webhook_dispatcher.py
├── parsers
│   ├── base_parser.py
│   ├── csv_parser.py
│   ├── edi_270_271_parser.py
│   ├── edi_834_parser.py
│   ├── edi_835_parser.py
│   ├── edi_837_parser.py
│   ├── edi_parser.py
│   ├── edi_utils.py
│   ├── fixed_width_parser.py
│   ├── hl7_parser.py
│   ├── __init__.py
│   ├── json_parser.py
│   ├── parquet_parser.py
│   └── xml_parser.py
├── README.md
├── requirements.txt
├── run.sh
├── sample_data
│   ├── csv
│   │   ├── member_eligibility.csv
│   │   ├── partner_orders.csv
│   │   └── weather_stations.csv
│   ├── edi
│   │   ├── sample_810_997.edi
│   │   ├── sample_850.edi
│   │   └── sample_856.edi
│   ├── healthcare
│   │   ├── fhir
│   │   ├── health_edi
│   │   │   ├── sample_270_271.edi
│   │   │   ├── sample_834.edi
│   │   │   ├── sample_835.edi
│   │   │   └── sample_837p.edi
│   │   ├── hl7
│   │   └── x12
│   ├── json
│   │   ├── okta_users.json
│   │   ├── order_webhook_event.json
│   │   └── weather_response.json
│   ├── parquet
│   │   ├── claims_export.parquet
│   │   ├── generate_samples.py
│   │   └── weather_observations.parquet
│   ├── text
│   │   ├── member_dump.txt
│   │   └── nacha_payroll.ach
│   └── xml
│       ├── order_status_soap.xml
│       └── product_catalog.xml
├── setup.sh
├── shared
│   ├── audit_log.py
│   ├── hmac_signer.py
│   ├── idempotency.py
│   └── retry_queue.py
├── structure.md
├── teardown.sh
└── tests
    ├── test_edi_parser.py
    ├── test_integrations.py
    └── test_parsers.py

23 directories, 80 files

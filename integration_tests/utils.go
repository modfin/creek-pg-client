package integration_tests

import (
	"context"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/modfin/creek"
)

var start sync.Once
var conn *creek.Conn
var connOnce sync.Once
var testCtx context.Context

func Schema(table string) avro.Schema {
	if table == "types" {
		return avro.MustParse(`
{
    "name": "publish_message",
    "type": "record",
    "fields": [
        {
            "name": "fingerprint",
            "type": "string"
        },
        {
            "name": "source",
            "type": {
                "name": "source",
                "type": "record",
                "fields": [
                    {
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "name": "tx_at",
                        "type": {
                            "type": "long",
                            "logicalType": "timestamp-micros"
                        }
                    },
                    {
                        "name": "db",
                        "type": "string"
                    },
                    {
                        "name": "schema",
                        "type": "string"
                    },
                    {
                        "name": "table",
                        "type": "string"
                    },
                    {
                        "name": "tx_id",
                        "type": "long"
                    },
                    {
                        "name": "last_lsn",
                        "type": "string"
                    },
                    {
                        "name": "lsn",
                        "type": "string"
                    }
                ]
            }
        },
        {
            "name": "op",
            "type": {
                "name": "op",
                "type": "enum",
                "symbols": [
                    "c",
                    "u",
                    "u_pk",
                    "d",
                    "t",
                    "r"
                ]
            }
        },
        {
            "name": "sent_at",
            "type": {
                "type": "long",
                "logicalType": "timestamp-micros"
            }
        },
        {
            "name": "before",
            "type": [
                "null",
                {
                    "name": "before.types",
                    "type": "record",
                    "fields": [
                        {
                            "name": "uuid",
                            "type": {
                                "type": "string",
                                "logicalType": "uuid"
                            },
                            "pgKey": true,
                            "pgType": "uuid"
                        }
                    ]
                }
            ]
        },
        {
            "name": "after",
            "type": [
                "null",
                {
                    "name": "after.types",
                    "type": "record",
                    "fields": [
                        {
                            "name": "bool",
                            "type": [
                                "null",
                                "boolean"
                            ],
                            "pgKey": false,
                            "pgType": "bool"
                        },
                        {
                            "name": "char",
                            "type": [
                                "null",
                                "string"
                            ],
                            "pgKey": false,
                            "pgType": "bpchar"
                        },
                        {
                            "name": "varchar",
                            "type": [
                                "null",
                                "string"
                            ],
                            "pgKey": false,
                            "pgType": "varchar"
                        },
                        {
                            "name": "bpchar",
                            "type": [
                                "null",
                                "string"
                            ],
                            "pgKey": false,
                            "pgType": "bpchar"
                        },
                        {
                            "name": "date",
                            "type": [
                                "null",
                                {
                                    "type": "int",
                                    "logicalType": "date"
                                },
                                {
                                    "name": "infinity_modifier",
                                    "type": "enum",
                                    "symbols": [
                                        "infinity",
                                        "negative_infinity_ca5991f51367e3e4"
                                    ]
                                }
                            ],
                            "pgKey": false,
                            "pgType": "date"
                        },
                        {
                            "name": "float4",
                            "type": [
                                "null",
                                "float"
                            ],
                            "pgKey": false,
                            "pgType": "float4"
                        },
                        {
                            "name": "float8",
                            "type": [
                                "null",
                                "double"
                            ],
                            "pgKey": false,
                            "pgType": "float8"
                        },
                        {
                            "name": "int2",
                            "type": [
                                "null",
                                "int"
                            ],
                            "pgKey": false,
                            "pgType": "int2"
                        },
                        {
                            "name": "int4",
                            "type": [
                                "null",
                                "int"
                            ],
                            "pgKey": false,
                            "pgType": "int4"
                        },
                        {
                            "name": "int8",
                            "type": [
                                "null",
                                "long"
                            ],
                            "pgKey": false,
                            "pgType": "int8"
                        },
                        {
                            "name": "json",
                            "type": [
                                "null",
                                "bytes"
                            ],
                            "pgKey": false,
                            "pgType": "json"
                        },
                        {
                            "name": "jsonb",
                            "type": [
                                "null",
                                "bytes"
                            ],
                            "pgKey": false,
                            "pgType": "jsonb"
                        },
                        {
                            "name": "text",
                            "type": [
                                "null",
                                "string"
                            ],
                            "pgKey": false,
                            "pgType": "text"
                        },
                        {
                            "name": "time",
                            "type": [
                                "null",
                                {
                                    "type": "long",
                                    "logicalType": "time-micros"
                                },
                                "after.infinity_modifier"
                            ],
                            "pgKey": false,
                            "pgType": "time"
                        },
                        {
                            "name": "timestamp",
                            "type": [
                                "null",
                                {
                                    "type": "long",
                                    "logicalType": "timestamp-micros"
                                },
                                "after.infinity_modifier"
                            ],
                            "pgKey": false,
                            "pgType": "timestamp"
                        },
                        {
                            "name": "timestamptz",
                            "type": [
                                "null",
                                {
                                    "type": "long",
                                    "logicalType": "timestamp-micros"
                                },
                                "after.infinity_modifier"
                            ],
                            "pgKey": false,
                            "pgType": "timestamptz"
                        },
                        {
                            "name": "uuid",
                            "type": {
                                "type": "string",
                                "logicalType": "uuid"
                            },
                            "pgKey": true,
                            "pgType": "uuid"
                        },
                        {
                            "name": "numeric",
                            "type": [
                                "null",
                                {
                                    "type": "bytes",
                                    "logicalType": "decimal",
                                    "precision": 10,
									"scale": 5
                                }
                            ],
                            "pgKey": false,
                            "pgType": "numeric"
                        },
                        {
                            "name": "boolarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "boolean"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_bool"
                        },
                        {
                            "name": "chararr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "string"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_bpchar"
                        },
                        {
                            "name": "varchararr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "string"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_varchar"
                        },
                        {
                            "name": "bpchararr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "string"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_bpchar"
                        },
                        {
                            "name": "datearr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": [
                                        {
                                            "type": "int",
                                            "logicalType": "date"
                                        },
                                        "after.infinity_modifier"
                                    ]
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_date"
                        },
                        {
                            "name": "float4arr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "float"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_float4"
                        },
                        {
                            "name": "float8arr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "double"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_float8"
                        },
                        {
                            "name": "int2arr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "int"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_int2"
                        },
                        {
                            "name": "int4arr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "int"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_int4"
                        },
                        {
                            "name": "int8arr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "long"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_int8"
                        },
                        {
                            "name": "jsonarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "bytes"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_json"
                        },
                        {
                            "name": "jsonbarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "bytes"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_jsonb"
                        },
                        {
                            "name": "textarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "string"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_text"
                        },
                        {
                            "name": "timearr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": [
                                        {
                                            "type": "long",
                                            "logicalType": "time-micros"
                                        },
                                        "after.infinity_modifier"
                                    ]
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_time"
                        },
                        {
                            "name": "timestamparr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": [
                                        {
                                            "type": "long",
                                            "logicalType": "timestamp-micros"
                                        },
                                        "after.infinity_modifier"
                                    ]
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_timestamp"
                        },
                        {
                            "name": "timestamptzarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": [
                                        {
                                            "type": "long",
                                            "logicalType": "timestamp-micros"
                                        },
                                        "after.infinity_modifier"
                                    ]
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_timestamptz"
                        },
                        {
                            "name": "uuidarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "string",
                                        "logicalType": "uuid"
                                    }
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_uuid"
                        },
                        {
                            "name": "numericarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "bytes",
                                        "logicalType": "decimal",
                                        "precision": 10,
										"scale": 5
                                    }
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_numeric"
                        }
                    ]
                }
            ]
        }
    ]
}`)
	}

	return nil
}

func Data(table string) []byte {
	if table == "types" {
		return []byte{
			0x18, 0x6d, 0x5f, 0x70, 0x70, 0x71, 0x67, 0x2d, 0x41, 0x4d, 0x6d, 0x30, 0x3d, 0x0a, 0x64, 0x75, 0x6d, 0x6d, 0x79, 0xf0, 0x83, 0x9e, 0xa9, 0xef, 0xc1, 0x81, 0x06, 0x0e, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x64, 0x62, 0x0c, 0x70, 0x75, 0x62, 0x6c, 0x69, 0x63, 0x0a, 0x74, 0x79, 0x70, 0x65, 0x73, 0xf0, 0x07, 0x12, 0x30, 0x2f, 0x31, 0x37, 0x46, 0x41, 0x30, 0x45, 0x35, 0x12, 0x30, 0x2f, 0x31, 0x37, 0x46, 0x41, 0x33, 0x33, 0x37, 0x00, 0x8a, 0xf4, 0x9e, 0xa9, 0xef, 0xc1, 0x81, 0x06, 0x00, 0x02, 0x02, 0x01, 0x02, 0x02, 0x61, 0x02, 0x04, 0x68, 0x69, 0x02, 0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x02, 0xe8, 0xae, 0x02, 0x02, 0x1f, 0x85, 0x6b, 0x3e, 0x02, 0xa4, 0x70, 0x3d, 0x0a, 0xd7, 0xa3, 0x28, 0x40, 0x02, 0xf6, 0x01, 0x02, 0xce, 0x03, 0x02, 0xe6, 0x83, 0x0f, 0x02, 0x22, 0x7b, 0x22, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x22, 0x3a, 0x22, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x22, 0x7d, 0x02, 0x22, 0x7b, 0x22, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x22, 0x3a, 0x22, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x22, 0x7d, 0x02, 0x08, 0x74, 0x65, 0x78, 0x74, 0x02, 0x80, 0x95, 0xf5, 0x2a, 0x02, 0x92, 0xfd, 0x9d, 0xa9, 0xef, 0xc1, 0x81, 0x06, 0x02, 0x92, 0xfd, 0x9d, 0xa9, 0xef, 0xc1, 0x81, 0x06, 0x48, 0x33, 0x64, 0x63, 0x66, 0x38, 0x37, 0x35, 0x32, 0x2d, 0x62, 0x61, 0x65, 0x64, 0x2d, 0x34, 0x61, 0x38, 0x34, 0x2d, 0x39, 0x65, 0x66, 0x39, 0x2d, 0x38, 0x33, 0x37, 0x37, 0x37, 0x38, 0x39, 0x34, 0x35, 0x66, 0x38, 0x66, 0x02, 0x08, 0x01, 0x60, 0xa6, 0x20, 0x02, 0x03, 0x04, 0x01, 0x00, 0x00, 0x02, 0x01, 0x04, 0x02, 0x61, 0x00, 0x02, 0x01, 0x06, 0x04, 0x68, 0x69, 0x00, 0x02, 0x01, 0x0c, 0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x02, 0x01, 0x08, 0x00, 0xe8, 0xae, 0x02, 0x00, 0x02, 0x01, 0x08, 0x1f, 0x85, 0x6b, 0x3e, 0x00, 0x02, 0x01, 0x10, 0xa4, 0x70, 0x3d, 0x0a, 0xd7, 0xa3, 0x28, 0x40, 0x00, 0x02, 0x01, 0x04, 0xf6, 0x01, 0x00, 0x02, 0x01, 0x04, 0xce, 0x03, 0x00, 0x02, 0x01, 0x06, 0xe6, 0x83, 0x0f, 0x00, 0x02, 0x00, 0x02, 0x00, 0x02, 0x01, 0x0a, 0x08, 0x74, 0x65, 0x78, 0x74, 0x00, 0x02, 0x03, 0x14, 0x00, 0x80, 0x95, 0xf5, 0x2a, 0x00, 0x80, 0xb0, 0xe3, 0x2d, 0x00, 0x02, 0x01, 0x12, 0x00, 0xf4, 0xf0, 0x9d, 0xa9, 0xef, 0xc1, 0x81, 0x06, 0x00, 0x02, 0x03, 0x24, 0x00, 0xf4, 0xf0, 0x9d, 0xa9, 0xef, 0xc1, 0x81, 0x06, 0x00, 0xf4, 0xf0, 0x9d, 0xa9, 0xef, 0xc1, 0x81, 0x06, 0x00, 0x02, 0x01, 0x4a, 0x48, 0x34, 0x36, 0x31, 0x34, 0x35, 0x64, 0x30, 0x35, 0x2d, 0x38, 0x62, 0x63, 0x37, 0x2d, 0x34, 0x30, 0x33, 0x62, 0x2d, 0x38, 0x30, 0x39, 0x38, 0x2d, 0x31, 0x62, 0x61, 0x66, 0x39, 0x39, 0x65, 0x39, 0x37, 0x62, 0x35, 0x36, 0x00, 0x02, 0x01, 0x0a, 0x08, 0x01, 0x60, 0xa6, 0x20, 0x00,
		}
	}
	return []byte{}
}

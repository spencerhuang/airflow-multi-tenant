// MongoDB Initialization Script
// This script runs automatically when MongoDB container starts
// Creates database, collection, and indexes for testing

print('========================================');
print('Starting MongoDB Initialization');
print('========================================');

// Switch to test database
db = db.getSiblingDB('test_database');

// Create test collection with validation schema
db.createCollection('test_s3_data', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['id', 'name'],
            properties: {
                id: {
                    bsonType: 'int',
                    description: 'must be an integer and is required'
                },
                name: {
                    bsonType: 'string',
                    description: 'must be a string and is required'
                },
                value: {
                    bsonType: 'int',
                    description: 'must be an integer'
                },
                timestamp: {
                    bsonType: 'date',
                    description: 'must be a date'
                },
                metadata: {
                    bsonType: 'object',
                    description: 'optional metadata object'
                }
            }
        }
    }
});

// Create indexes for better query performance
db.test_s3_data.createIndex({ id: 1 }, { unique: true });
db.test_s3_data.createIndex({ name: 1 });
db.test_s3_data.createIndex({ timestamp: -1 });

print('✓ Created collection: test_s3_data');
print('✓ Created indexes on id, name, and timestamp');

// Create a sample collection for production use
db.createCollection('s3_imports', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['source_bucket', 'source_key', 'imported_at'],
            properties: {
                source_bucket: {
                    bsonType: 'string',
                    description: 'S3 bucket name'
                },
                source_key: {
                    bsonType: 'string',
                    description: 'S3 object key'
                },
                imported_at: {
                    bsonType: 'date',
                    description: 'Import timestamp'
                },
                data: {
                    bsonType: 'array',
                    description: 'Imported data records'
                }
            }
        }
    }
});

db.s3_imports.createIndex({ source_bucket: 1, source_key: 1 }, { unique: true });
db.s3_imports.createIndex({ imported_at: -1 });

print('✓ Created collection: s3_imports');
print('✓ Created indexes for import tracking');

// Show created collections
print('\nCreated collections:');
db.getCollectionNames().forEach(function(collection) {
    print('  - ' + collection);
});

print('\n========================================');
print('MongoDB Initialization Complete');
print('========================================');

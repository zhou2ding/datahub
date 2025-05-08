**gRPC DataLayer Service**  
A high-performance **gRPC-based data access layer** with unified CRUD, transactions, caching (Redis), and schema inspection. Supports complex queries (joins, aggregations) and multi-database backends. Perfect for microservices needing a clean data abstraction.

**Key Features**:
- 📊 Standardized Query/Insert/Update/Delete
- 🔄 Transaction support (Begin/Commit/Rollback)
- ⚡ Redis caching with TTL & multi-DB isolation
- 🔍 Metadata API (list tables, describe schema)
- 🧩 Protobuf-defined interface (database-agnostic)

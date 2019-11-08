## eneel 0.1.2 (November 8, 2019)

### Overview

Introduce new functionallity in queries and changes to the export technology.

### Breaking changes
 - source_columntypes_to_exclude is replaced with internal removing of unsupported columntypes.

### Features:
 - Load queries
 - Better interpretation of data types between databases
 - Added tests
 - Database logging of load progress
 - Parallel processing of table loads
 
### Fixes:
- Feedback when project.yml not found
- Better feedback when first run incremental

### Changed:
- Using database adapter query for exports from postgres and oracle. Keeping bcp for sql server due to performance

## eneel 0.1.1 (September 28, 2019)

### Overview

Fix release

### Fixes:
- Performance with Oracle exports
- Better logging
- Fixed issue with load order numbering


## eneel 0.1.0 (September 25, 2019)

### Overview

Initial release.
ed issue with load order numbering
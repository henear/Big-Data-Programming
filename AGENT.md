# AGENT.md

## Project Overview
- Maven Java project for Big Data Programming coursework.
- Hadoop assignments: `assign1` to `assign8`.
- Spark assignments: `assign9` to `assign12`.

## Build and Test
- Compile: `mvn -q -DskipTests clean compile`
- Run tests: `mvn test`
- Package: `mvn -DskipTests package`

## Dependency Notes
- Current key versions are managed in `pom.xml`:
  - Hadoop `3.4.2`
  - Spark `3.5.6` (`_2.12` artifacts)
  - Avro runtime `1.11.4`
- `avro-maven-plugin` is intentionally pinned to `1.8.1` for schema-generation compatibility with existing `.avsc` defaults.

## Testing Compatibility
- Unit tests use legacy MRUnit + JUnit 4.
- Surefire is configured with:
  - `--add-opens java.base/java.lang=ALL-UNNAMED`
- Do not remove this unless MRUnit tests are migrated.

## Security/Vulnerability Workflow
- Use OWASP dependency check when auditing:
  - `mvn org.owasp:dependency-check-maven:check -Dformat=JSON -DautoUpdate=false`
- Prioritize fixing direct dependencies first; many findings are transitive from Hadoop/Spark ecosystems.

## Repo Hygiene
- Do **not** commit generated artifacts in `target/`.
- Avoid committing IDE-local files in `.idea/`.
- Keep changes minimal and scoped to the requested task.

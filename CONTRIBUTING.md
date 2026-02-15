# Contributing to armada-spark

## Development Setup

**Prerequisites:** Java 17, Maven 3.9.6+, Scala 2.13.8

```bash
mvn clean package          # Build with tests
mvn test                   # Run unit tests only
mvn spotless:check         # Check formatting
mvn spotless:apply         # Auto-fix formatting
scripts/dev-e2e.sh         # Run E2E tests (requires a running Armada cluster)

# Target a different Spark/Scala version (e.g., Spark 3.3.4, Scala 2.12.15)
./scripts/set-version.sh 3.3.4 2.12.15
```

## Commit Conventions

This project follows [Conventional Commits v1.0.0](https://www.conventionalcommits.org/en/v1.0.0/). All commits must be signed off:

```bash
git commit --signoff -m "feat(allocator): support configurable batch size"
```

Common scopes: `config`, `submit`, `allocator`, `event-watcher`, `e2e`, `docker`, `ci`.

## Pull Requests

1. Branch from `master` (`git checkout -b feat/my-feature master`)
2. Format code (`mvn spotless:apply`) and run tests (`mvn test`)
3. Commit using conventional commits with `--signoff`
4. Open a PR against `master` — the PR title should also follow conventional commit format

### PR Checklist

- [ ] `mvn clean package` compiles
- [ ] `mvn test` passes
- [ ] `mvn spotless:check` passes
- [ ] Commits follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
- [ ] New code includes tests

CI runs lint, build (Spark 3.3/3.5 x Scala 2.12/2.13), snapshots, and E2E tests on every PR. All checks must pass.

## Coding Standards

- **Scalafmt** enforced (100-col limit) — Apache 2.0 license header required on all source files
- **Naming:** `PascalCase` classes, `camelCase` methods, `UPPER_SNAKE_CASE` config constants, `{ClassName}Suite` test files
- **Patterns:** `Option`/`Try` over null/exceptions, `private[spark]` scoped visibility, Spark's `Logging` trait
- **Imports:** Java/javax → Scala stdlib → Third-party → Spark
- **Testing:** ScalaTest `AnyFunSuite` + Mockito (`mock(classOf[Type])`), `TableDrivenPropertyChecks` for parameterized tests
- **Version-specific code** lives in `src/main/scala-spark-{3.3,3.5,4.1}/` — changes to submission logic likely need updates in all version directories

## Working with Claude Code (Optional)

This project includes a [Claude Code](https://docs.anthropic.com/en/docs/claude-code) setup. Shared config (`.claude/settings.json`, `.claude/commands/`, `CLAUDE.md`) is checked into git. Hooks auto-format code and verify builds.

| Command      | What it does                                    |
|--------------|-------------------------------------------------|
| `/build`     | Build the project (`/build fast` to skip tests) |
| `/lint`      | Check and auto-fix formatting                   |
| `/commit`    | Create a conventional commit                    |
| `/ci-local`  | Run the full CI pipeline locally                |
| `/summary`   | Generate a PR description from your branch      |
| `/issue`     | Draft a GitHub issue from bug details/logs      |

**Optional plugins:** Copy `.claude/settings.local.example.json` to `.claude/settings.local.json` and restart. Source: https://github.com/wshobson/agents

## Getting Help

- Open a GitHub issue for bugs or feature requests
- For Armada questions, see [armadaproject.io](https://armadaproject.io/)

# apms-etest-admin-ibudatapm-demo

## SonarCloud setup

This repository is configured for SonarCloud analysis via GitHub Actions.

### 1) Update SonarCloud placeholders

Edit `sonar-project.properties` and replace:

- `REPLACE_WITH_SONARCLOUD_PROJECT_KEY`
- `REPLACE_WITH_SONARCLOUD_ORG_KEY`
- `REPLACE_WITH_PROJECT_DISPLAY_NAME`

### 2) Add repository secret

In GitHub repository settings, add:

- `SONAR_TOKEN` = your SonarCloud project (or organization) token

### 3) Trigger analysis

- Push to `main` / `master`, or
- Open/update a pull request

The workflow file is located at:

- `.github/workflows/sonarcloud.yml`

### Optional badge (replace placeholders first)

```md
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=REPLACE_WITH_SONARCLOUD_PROJECT_KEY&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=REPLACE_WITH_SONARCLOUD_PROJECT_KEY)
```

# apms-etest-admin-ibudatapm-demo

## Repository assessment dashboard

A downloadable architecture, code quality, security, and governance assessment is available at:

- [assessment/repo-assessment-dashboard.html](assessment/repo-assessment-dashboard.html)

Use the link above to access the report in the repository, or download the HTML file and open it locally in a browser for the full rendered dashboard view.

Assessment summary:

- Overall score: 4.8 / 10
- Strongest area: Design & Data Architecture (6.2 / 10)
- Weakest areas: Governance (4.0 / 10) and Security (4.1 / 10)

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

# Additions to helm/charts/iggy/Chart.yaml

Append to the existing Chart.yaml (no existing keys change):

```yaml
home: https://iggy.apache.org
icon: https://iggy.apache.org/img/iggy-logo.svg   # confirm the path on the website before merging
annotations:
  artifacthub.io/license: Apache-2.0
  artifacthub.io/category: streaming-messaging
  artifacthub.io/links: |
    - name: Documentation
      url: https://iggy.apache.org/docs/
    - name: Source
      url: https://github.com/apache/iggy/tree/master/helm/charts/iggy
  artifacthub.io/images: |
    - name: iggy
      image: apache/iggy
    - name: iggy-web-ui
      image: apache/iggy-web-ui
```

And add an "Install from the OCI registry" section to README.md.gotmpl (README.md is generated from it):

```markdown
### From the OCI registry

helm install iggy oci://ghcr.io/apache/iggy/charts/iggy --version <chart version>
```

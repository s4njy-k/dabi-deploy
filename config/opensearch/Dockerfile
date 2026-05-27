# Custom OpenSearch image: upstream + repository-gcs plugin baked in
# so snapshot backups to gs://dabi-prod-backup survive container recreation.
# Plugin needs OPENSEARCH_JAVA_OPTS cleared at install time because jvm.options
# already sets a GC and the install CLI would otherwise fail with
# "Multiple garbage collectors selected".
FROM opensearchproject/opensearch:3
RUN OPENSEARCH_JAVA_OPTS="" bin/opensearch-plugin install --batch repository-gcs

{{/*
Standard labels for eduide-shared-cache resources
*/}}
{{- define "eduide-shared-cache.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "eduide-shared-cache.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Resolve the cache hostname: explicit gateway.cache.hostname wins,
otherwise derive `cache.<gateway.baseHost>`.
*/}}
{{- define "eduide-shared-cache.cacheHostname" -}}
{{- if .Values.gateway.cache.hostname -}}
{{ .Values.gateway.cache.hostname }}
{{- else if .Values.gateway.baseHost -}}
cache.{{ .Values.gateway.baseHost }}
{{- else -}}
{{ fail "Set gateway.baseHost or gateway.cache.hostname" }}
{{- end -}}
{{- end -}}

{{/*
Resolve the repo hostname: explicit gateway.repo.hostname wins,
otherwise derive `repo.<gateway.baseHost>`.
*/}}
{{- define "eduide-shared-cache.repoHostname" -}}
{{- if .Values.gateway.repo.hostname -}}
{{ .Values.gateway.repo.hostname }}
{{- else if .Values.gateway.baseHost -}}
repo.{{ .Values.gateway.baseHost }}
{{- else -}}
{{ fail "Set gateway.baseHost or gateway.repo.hostname" }}
{{- end -}}
{{- end -}}

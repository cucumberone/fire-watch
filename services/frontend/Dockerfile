# Unprivileged nginx for OpenShift.
#
# Port 8080, not 80: a random UID under the restricted SCC cannot bind a
# privileged port (docs/frontend.md §5).
FROM docker.io/nginxinc/nginx-unprivileged:1.27-alpine

# Copy the build output. The image is built AFTER `npm run build`.
COPY dist/ /usr/share/nginx/html/

# Server block: CSP, cache policy, SPA fallback, /healthz.
COPY nginx.conf /etc/nginx/conf.d/default.conf

# nginx-unprivileged already listens on 8080 and drops privileges; the
# deployment sets runAsUser, so nothing here needs to.
USER 101

EXPOSE 8080

# Probes hit /healthz, which the ConfigMap-mounted server block answers with a
# static 200. The frontend has no dependencies to check — a dependency-aware
# probe would restart pods during a gateway outage, exactly when the UI must
# stay up to show the reconnect state (docs/frontend.md §6).

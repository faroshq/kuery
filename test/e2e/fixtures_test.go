//go:build e2e

package e2e_test

// clusterAFixtures are the YAML manifests applied to the "alpha" cluster.
const clusterAFixtures = `
apiVersion: v1
kind: Secret
metadata:
  name: nginx-tls
  namespace: demo
  labels:
    app: nginx
type: Opaque
stringData:
  tls.crt: "fake-cert-data"
  tls.key: "fake-key-data"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: nginx-config
  namespace: demo
  labels:
    app: nginx
data:
  nginx.conf: "server { listen 80; }"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: demo
  labels:
    app: nginx
    kuery.io/group: my-app
  annotations:
    kuery.io/relates-to: '[{"cluster":"cluster-b","kind":"Secret","namespace":"demo","name":"shared-cert"}]'
data:
  key: value
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: nginx-sa
  namespace: demo
  labels:
    app: nginx
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: nginx-data
  namespace: demo
  labels:
    app: nginx
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx
  namespace: demo
  labels:
    app: nginx
    env: production
    kuery.io/group: my-app
spec:
  replicas: 2
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      serviceAccountName: nginx-sa
      containers:
        - name: nginx
          image: nginx:1.27
          ports:
            - containerPort: 80
          volumeMounts:
            - name: tls
              mountPath: /etc/tls
              readOnly: true
            - name: config
              mountPath: /etc/nginx/conf.d
            - name: data
              mountPath: /var/www/html
      volumes:
        - name: tls
          secret:
            secretName: nginx-tls
        - name: config
          configMap:
            name: nginx-config
        - name: data
          persistentVolumeClaim:
            claimName: nginx-data
---
apiVersion: v1
kind: Service
metadata:
  name: nginx-svc
  namespace: demo
  labels:
    app: nginx
spec:
  selector:
    app: nginx
  ports:
    - port: 80
      targetPort: 80
`

// clusterBFixtures are the YAML manifests applied to the "beta" cluster.
const clusterBFixtures = `
apiVersion: v1
kind: Secret
metadata:
  name: shared-cert
  namespace: demo
  labels:
    kuery.io/group: my-app
type: Opaque
stringData:
  cert: "shared-certificate-data"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: redis-config
  namespace: demo
  labels:
    app: redis
    kuery.io/group: my-app
data:
  redis.conf: "maxmemory 256mb"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  namespace: demo
  labels:
    app: redis
    env: staging
    kuery.io/group: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: redis:7
          ports:
            - containerPort: 6379
---
apiVersion: v1
kind: Service
metadata:
  name: redis-svc
  namespace: demo
  labels:
    app: redis
spec:
  selector:
    app: redis
  ports:
    - port: 6379
      targetPort: 6379
`

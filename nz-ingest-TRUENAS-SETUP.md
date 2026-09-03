# nz-ingest as a TrueNAS Custom App

This guide explains how to deploy nz-ingest as a proper TrueNAS SCALE app using a custom repository.

## Repository Structure

Your GitHub repo should have this structure to be a valid TrueNAS app catalog:

```
nz-ingest/
├── Chart.yaml              # App metadata
├── values.yaml             # Default configuration
├── templates/
│   ├── deployment.yaml     # Kubernetes deployment & service
│   └── _helpers.tpl        # Template helpers
├── app/                    # Your Python app
│   ├── ingestd.py
│   ├── mediacheck.py
│   └── store.py
├── icon.png                # App icon (512x512+)
├── README.md               # Technical README
└── CHANGELOG.md            # Version history
```

## Setup Steps

### 1. Prepare Your Repository

Create a new repository on GitHub (or use your existing nz-ingest repo) with the structure above.

**Repository Requirements:**
- Public (or private with token support)
- Helm chart files in the root directory
- Icon image (PNG, 512x512 or larger) named `icon.png`
- Update Chart.yaml with your GitHub username and email

### 2. Create an Icon

Use the Imperial logo (or create a custom icon):
- Minimum size: 512x512 pixels
- PNG format with transparency
- Save as `icon.png` in the repo root

### 3. Update Chart.yaml

Replace placeholders:
```yaml
home: https://github.com/YOUR_USER/nz-ingest
sources:
  - https://github.com/YOUR_USER/nz-ingest
icon: https://raw.githubusercontent.com/YOUR_USER/nz-ingest/main/icon.png
maintainers:
  - name: YOUR_NAME
    email: your@email.com
```

### 4. Version the Chart

Update version numbers in `Chart.yaml`:
```yaml
version: 1.6.1          # Chart version
appVersion: "1.6.1"     # Your app version
```

When you release a new version:
1. Update both `version` and `appVersion` in Chart.yaml
2. Push to GitHub with a git tag (e.g., `v1.6.1`)
3. TrueNAS will detect and offer the update

## Adding to TrueNAS

### For Users:

1. **In TrueNAS SCALE:**
   - Go to **Apps** → **Discover**
   - Click **+ Add Catalog** (upper right)
   - Fill in:
     - **Name:** `nz-ingest`
     - **Repository:** `https://github.com/YOUR_USER/nz-ingest`
     - **Branch:** `main` (or whatever you use)
     - **Preferred Namespace:** `default`

2. **Install the App:**
   - Go to **Apps** → **Discover**
   - Search for "nz-ingest"
   - Click **Install**
   - Configure storage paths for drop, archive, and data folders
   - Click **Install**

3. **Access the Web UI:**
   - Apps → Installed → nz-ingest
   - Note the port (usually 8077)
   - Open http://your-truenas-ip:8077

### For Custom Hosting:

If you want to host the app repository yourself (not on GitHub):

1. **Create a catalog index** — a simple YAML file listing available versions
2. **Host on a web server** or GitHub
3. **Users add your URL** as the catalog source

## Configuration

Users can customize settings when installing:

- **NZ_DROP** — Drop folder path (default: /drop)
- **NZ_ARCHIVE** — Archive root path (default: /archive)
- **NZ_DB** — Database path (default: /data/ingest.db)
- **NZ_PORT** — Web UI port (default: 8077)
- **NZ_SETTLE** — File copy settle time in seconds (default: 20)
- **NZ_READONLY** — Set to "1" for dry-run mode
- **NZ_UPDATE_BASE** — GitHub raw URL for auto-updates (optional)
- **NZ_UPDATE_TOKEN** — GitHub token for private repos (optional)

## Releasing Updates

### To release a new version:

1. **Update the app code** (ingestd.py, mediacheck.py, store.py)

2. **Update Chart.yaml:**
   ```yaml
   version: 1.6.2          # Increment the chart version
   appVersion: "1.6.2"     # Match your app version
   ```

3. **Commit and push:**
   ```bash
   git add Chart.yaml app/*
   git commit -m "Release nz-ingest 1.6.2"
   git tag v1.6.2
   git push origin main --tags
   ```

4. **Users will see the update** in TrueNAS:
   - Apps → Installed → nz-ingest
   - "Update available" banner appears
   - Click to install the new version

## Auto-Update Within the App

The app itself checks for updates from GitHub (if `NZ_UPDATE_BASE` is set):

```bash
NZ_UPDATE_BASE=https://raw.githubusercontent.com/YOUR_USER/nz-ingest/main/app
```

Users can then:
1. Open the web UI
2. Go to **Firmware** section
3. Click "check" → see available updates
4. Click "install & restart"

The app fetches code from GitHub and restarts itself.

## Troubleshooting

**App doesn't start after install**
- Check logs: Apps → nz-ingest → Logs
- Verify volume mounts exist and are readable
- Check that storage paths in `values.yaml` match actual dataset paths

**Web UI not accessible**
- Verify the pod is running: `kubectl get pods -n default | grep nz-ingest`
- Check service: `kubectl get svc -n default | grep nz-ingest`
- Verify port mapping in TrueNAS UI

**Update fails**
- Check that repository URL is accessible
- If private repo, verify GitHub token is set and valid
- Check logs for specific error messages

## Directory Layout for Deployment

When TrueNAS deploys your app, it will:

1. Clone your repo (or use the chart from cache)
2. Mount your specified datasets at the paths in `values.yaml`
3. Start the Python container with the environment variables
4. Expose the web UI on the service port

The container runs `/app/ingestd.py` which starts the daemon and web server.

## Support & Maintenance

- Document your app in the GitHub wiki
- Handle issues and feature requests
- Keep the Chart.yaml metadata current
- Test updates before tagging releases
- Provide clear CHANGELOG entries for each version

---

**For detailed Helm chart documentation**, see the [Helm docs](https://helm.sh/docs/).

**For TrueNAS app development**, see the [TrueNAS documentation](https://docs.truenas.com/).

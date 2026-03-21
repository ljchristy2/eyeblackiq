# EyeBlackIQ — GitHub Setup Guide
### Complete step-by-step implementation from zero to live website

---

## STEP 1 — Create The Repository

1. Go to `github.com` and log into your account
2. Click the **"+"** icon in the top right corner
3. Click **"New repository"**
4. Fill in the following:
   - **Repository name:** `eyeblackiq`
   - **Description:** `NCAA baseball prediction model. We find where the market is wrong.`
   - **Visibility:** ✅ Public
   - **Initialize with README:** ✅ Check this
   - Everything else: leave default
5. Click **"Create repository"**

---

## STEP 2 — Upload The Website Files

You have two options. Option A is easiest.

### Option A — Upload via GitHub Web Interface (Recommended)

1. Inside your new `eyeblackiq` repo, click **"Add file"** → **"Upload files"**
2. Drag and drop the entire `docs/` folder
3. Scroll down, write commit message: `Initial website build`
4. Click **"Commit changes"**

Then upload `README.md` the same way — it will replace the placeholder README.

### Option B — Upload via Git Command Line

If you have Git installed locally:

```bash
# Clone your repo
git clone https://github.com/YOUR_USERNAME/eyeblackiq.git
cd eyeblackiq

# Copy all files into this folder
# (drag docs/ and README.md into the eyeblackiq folder)

# Push everything
git add .
git commit -m "Initial website build"
git push origin main
```

---

## STEP 3 — Enable GitHub Pages

1. Inside your `eyeblackiq` repo, click **"Settings"** (top right tab)
2. In the left sidebar, scroll down to **"Pages"**
3. Under **"Source"**, select:
   - Branch: **main**
   - Folder: **/ (root)** → change this to **"/docs"**
4. Click **"Save"**
5. Wait 2-3 minutes
6. GitHub will show you your live URL:
   `https://YOUR_USERNAME.github.io/eyeblackiq`

---

## STEP 4 — Verify The Site Is Live

1. Go to `https://YOUR_USERNAME.github.io/eyeblackiq`
2. You should see the EyeBlackIQ landing page
3. It will say "Season Starting Soon" until the first slip is posted
4. Click through to Results and Methodology — both should load

If the site shows a 404 — wait 5 more minutes. GitHub Pages takes time to deploy on first launch.

---

## STEP 5 — Add Your Website URL To Twitter

1. Go to `x.com` → Profile → Edit Profile
2. Website field: paste `https://YOUR_USERNAME.github.io/eyeblackiq`
3. Save

Now every tweet links back to the full record and methodology.

---

## STEP 6 — How The Pipeline Updates The Website

When `morning_run.py` runs each day:
- It writes `docs/data/today_slip.json` with today's picks
- It pushes the updated file to GitHub
- The website automatically shows today's card

When `evening_run.py` runs each day:
- It appends results to `docs/data/results.json`
- It recalculates and writes `docs/data/record.json`
- It pushes both files to GitHub
- The website automatically updates the record bar and results tables

**No manual updates needed. The pipeline handles everything.**

---

## STEP 7 — Test The Data Files Manually (First Time)

Before the pipeline is connected, you can manually update the website by editing the JSON files directly in GitHub:

1. Go to your repo → `docs/data/today_slip.json`
2. Click the pencil icon (Edit)
3. Paste a test slip:

```json
{
  "date": "Sunday, March 1, 2026",
  "pod": {
    "pick": "OVER 11.5",
    "teams": "Illinois St / Mid TN",
    "line": "-110",
    "tier": "CONVICTION",
    "units": 2.0
  },
  "picks": [
    {
      "pick": "OVER 11.5",
      "teams": "Illinois St / Mid TN",
      "line": "-110",
      "tier": "CONVICTION",
      "units": 2.0
    },
    {
      "pick": "OVER 10.5",
      "teams": "Nebraska-Omaha / Wichita",
      "line": "-115",
      "tier": "VALUE",
      "units": 1.5
    },
    {
      "pick": "Arizona ML",
      "teams": "Arizona vs USC",
      "line": "+225",
      "tier": "VALUE",
      "units": 1.0
    }
  ]
}
```

4. Commit changes
5. Refresh the website in 30 seconds — picks will appear

---

## TROUBLESHOOTING

**Site shows 404:**
- Wait 5-10 minutes after enabling Pages
- Make sure the folder is set to `/docs` not `/ (root)`
- Make sure `index.html` is inside the `docs/` folder

**Picks not loading:**
- Check `docs/data/today_slip.json` exists in the repo
- Make sure the JSON is valid (no trailing commas)
- Hard refresh the browser: Ctrl+Shift+R

**Record bar showing zeros:**
- This is correct until `record.json` is updated by the pipeline
- Manually edit `docs/data/record.json` to test

**GitHub Pages URL:**
- Format is always: `https://USERNAME.github.io/REPONAME`
- Replace USERNAME with your actual GitHub username
- Replace REPONAME with `eyeblackiq`

---

## YOUR LIVE URL

Once setup is complete, your site lives at:

```
https://YOUR_GITHUB_USERNAME.github.io/eyeblackiq
```

Add this to:
- ✅ Twitter bio
- ✅ Telegram channel description  
- ✅ Discord server description
- ✅ Every results post: "Full record → [URL]"

---

*EyeBlackIQ — The model sees what the market misses.*

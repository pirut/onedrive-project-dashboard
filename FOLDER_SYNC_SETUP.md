# Folder Sync to FastField Setup Guide

This guide will help you set up automatic hourly syncing of your OneDrive/SharePoint folder names to FastField as data table items.

## What's Been Created

1. **`api/sync-folders-cron.js`** - The main function that runs hourly via Vercel Cron
2. **`vercel.json`** - Configuration for the cron job (runs every hour at minute 0)
3. **`scripts/test-folder-sync.js`** - Test script to verify everything works
4. **`FOLDER_SYNC_SETUP.md`** - This setup guide

## Step 1: Get FastField API Details

You need to provide the following information from FastField:

1. **FastField API Endpoint URL** - Where to send the folder data
2. **API Key or Authentication Token** - How to authenticate with FastField
3. **Data Table Name** - The name of the table in FastField where folders will be stored

### Questions for FastField Support:

1. "What's the API endpoint for updating data tables?"
2. "What authentication method do you use (API key, Bearer token, etc.)?"
3. "What's the expected payload format for updating data tables?"
4. "Should I use 'replace' or 'append' action for the data?"

## Step 2: Configure Environment Variables

Add these to your `.env` file and Vercel environment:

```bash
# Existing variables (you should already have these)
TENANT_ID=your-tenant-id
MSAL_CLIENT_ID=your-client-id
MSAL_CLIENT_SECRET=your-client-secret
DEFAULT_SITE_URL=https://yourtenant.sharepoint.com/sites/work
DEFAULT_LIBRARY=Documents/Cornerstone Jobs

# New FastField variables (you need to provide these)
FASTFIELD_API_URL=https://api.fastfieldforms.com/services/v3/datatables/
FASTFIELD_API_KEY=08c75cee57ac40afbad2909ce48c68c4
FASTFIELD_TABLE_NAME=Cornerstone Active Projects
```

## Step 3: Test the Setup

1. **Test locally first:**

    ```bash
    npm run test-sync
    ```

2. **Test the API endpoint manually:**
    ```bash
    curl -X POST https://your-vercel-app.vercel.app/api/sync-folders-cron
    ```

## Step 4: Deploy to Vercel

```bash
vercel --prod
```

The cron job will automatically start running every hour.

## Step 5: Monitor the Sync

You can monitor the sync in several ways:

1. **Vercel Function Logs:**

    - Go to your Vercel dashboard
    - Click on your project
    - Go to Functions tab
    - Click on `sync-folders-cron.js`
    - View the logs

2. **Manual Testing:**

    ```bash
    curl -X POST https://your-vercel-app.vercel.app/api/sync-folders-cron
    ```

3. **Check your KV logs** (if you have KV configured):
    - The function logs all sync attempts to your KV store

## Data Format

The function sends individual folder data to FastField using their datatable API. Each folder is sent as a separate API call with this structure:

```json
{
    "Project Name": "Project Folder Name",
    "Folder ID": "unique-folder-id",
    "Folder URL": "https://sharepoint.com/...",
    "Created Date": "2024-01-01",
    "Last Modified": "2024-01-01",
    "Size (bytes)": 1024,
    "Drive ID": "drive-id",
    "Parent Path": "/path/to/parent",
    "Last Synced": "2024-01-01",
    "Status": "Active"
}
```

**Note:** The function processes folders in batches of 10 with delays between requests to be respectful to the FastField API.

## Customization Options

### Change Sync Frequency

Edit `vercel.json` to change the cron schedule:

```json
{
    "functions": {
        "api/sync-folders-cron.js": {
            "cron": "0 */2 * * *" // Every 2 hours
            // "cron": "0 9 * * *"  // Daily at 9 AM
            // "cron": "0 9 * * 1"  // Weekly on Monday at 9 AM
        }
    }
}
```

### Filter Folders

Edit the filter logic in `api/sync-folders-cron.js`:

```javascript
.filter((it) => {
    const name = (it.name || "").trim().toLowerCase();
    // Add your custom filters here
    return !name.includes("(archive)") &&
           !name.includes("archive") &&
           !name.includes("temp") &&
           name.length > 0;
})
```

### Add More Metadata

Modify the data mapping in the `syncToFastField` function to include additional fields.

## Troubleshooting

### Common Issues

1. **Authentication Errors:**

    - Check your Microsoft Graph permissions
    - Verify `TENANT_ID`, `MSAL_CLIENT_ID`, `MSAL_CLIENT_SECRET`

2. **FastField API Errors:**

    - Verify `FASTFIELD_API_URL` and `FASTFIELD_API_KEY`
    - Check the expected payload format with FastField support

3. **No Folders Found:**
    - Verify `DEFAULT_SITE_URL` and `DEFAULT_LIBRARY`
    - Check if the library path exists and has folders

### Debug Mode

To get more detailed logs, you can temporarily add this to your function:

```javascript
console.log("Debug info:", {
    siteUrl,
    libraryPath,
    foldersCount: folders.length,
    sampleFolders: folders.slice(0, 3),
});
```

## Support

If you encounter issues:

1. Check the Vercel function logs
2. Run the test script locally: `npm run test-sync`
3. Verify all environment variables are set correctly
4. Contact FastField support for API-specific questions

# Grant Log Sink Permissions to Write to BigQuery Table and Enable Data Access Logs

This guide explains how to grant a log sink the necessary permissions to write to a BigQuery table, and enable Data Access logs. It also includes instructions for retrieving the **Writer Identity**.

---

## **Retrieve Writer Identity**

### **Via Google Cloud Console (UI)**

1. Navigate to **Logging** in the Google Cloud Console.
2. Go to the **Log Router** section.
3. Locate the sink with the name `data-catalog-audit-logs`.
4. Click the **More actions** button (three vertical dots on the right).
5. Select **View sink details**.
6. Copy the **Writer Identity** displayed in the sink details (e.g., `service-123456789012@gcp-sa-logging.iam.gserviceaccount.com`).

### **Using Cloud Shell Terminal**

1. Retrieve the sink's **Writer Identity** by describing the sink:
   ```bash
   gcloud logging sinks describe SINK_NAME
   ```
   Example:
   ```bash
   gcloud logging sinks describe projects/<work_project_id>/sinks/data-catalog-audit-logs
   ```
   The Writer Identity will look like:
   ```
   serviceAccount:service-123456789012@gcp-sa-logging.iam.gserviceaccount.com
   ```

---

## **Grant Permissions**

### **Via Google Cloud Console (UI)**

1. Navigate to **IAM & Admin** in the Google Cloud Console.
2. Click **Grant Access**.
3. Add the sink's **Writer Identity** (e.g., `service-123456789012@gcp-sa-logging.iam.gserviceaccount.com`) as a **principal**.
4. Assign the **BigQuery Data Editor** role (`roles/bigquery.dataEditor`) to the principal.

### **Using Cloud Shell Terminal**

1. Add the service account as an IAM principal with the required role:
   ```bash
   gcloud projects add-iam-policy-binding <work_project_id> \
       --member=<serviceAccount:service-123456789012@gcp-sa-logging.iam.gserviceaccount.com> \
       --role=roles/bigquery.dataEditor
   ```
   Replace `<serviceAccount:service-123456789012@gcp-sa-logging.iam.gserviceaccount.com>` with your actual Writer Identity.

---

## **Enable Data Access Logs**

### **Via Google Cloud Console (UI)**

1. In the Google Cloud Console, go to the **Audit Logs** page.
2. In the **Data Access audit logs configuration** table, locate **Data Catalog** in the **Service** column.
3. Click on **Data Catalog** to open the configuration panel.
4. In the **Log Types** tab, select the Data Access audit log types you want to enable:
   - **DATA_READ**
   - **DATA_WRITE**
   - **ADMIN_READ**
5. Go to the **Exempted Principals** tab in the information panel.
6. Click **Add exempted principal** and add the service account used by transfer tooling to improve log readability (e.g., `service-account@project-id.iam.gserviceaccount.com`).
7. In the **Disabled permission types** section for the exempted principal, select audit log types that you want to disable:
   - **ADMIN_READ**
   - **DATA_READ**
   - **DATA_WRITE**
8. Click **Done**.
9. Click **Save**.

> **Note:** Enabling Data Access logs via the UI is safer because it ensures that existing policies are preserved. Using Cloud Shell may unintentionally override existing configurations.

## Troubleshooting
### Issue: Unable to Add Exempted Principal
If you encounter an issue when adding an exempted principal, it may be due to the `iam.disableAuditLoggingExemption` organization policy being enforced. This policy prevents adding exemptions to audit logs.

### Solution: Disable the Policy
1. Navigate to **IAM & Admin** > **Organization Policies** in the **Google Cloud Console**.
2. Locate the policy `iam.disableAuditLoggingExemption`.
3. Click on the policy to open it, then click **Manage Policy**. Alternatively, click the **Actions** menu (three vertical dots) next to the policy and select **Edit Policy**. As shown in the image below, change the policy setting to disable the constraint.\
   ![disable_audit_log_exemption.png](/pictures/disable_audit_log_exemption.png)
4. Save the changes and retry adding the exempted principal.

def find_unused_broker_doc_categories(final_json):
    # Full set of all possible broker document categories
    all_categories = {
        # Income Documents
        'Payslips',
        'PAYG Summary',
        'Tax Return',
        'Notice of Assessment',
        'Employment Contract',
        'Employment Letter',

        # Bank & Financial Documents
        'Bank Statements',
        'Credit Card Statements',
        'Loan Statements',
        'ATO Debt Statement',
        'HECS/HELP Debt',

        # ID & Verification Documents
        'Driver’s Licence',
        'Passport',
        'Medicare Card',
        'Birth Certificate',
        'Citizenship Certificate',
        'VOI Certificate',

        # Property-Related Documents
        'Contract of Sale',
        'Building Contract',
        'Plans and Specifications',
        'Council Approval',
        'Deposit Receipt',
        'Transfer Document',
        'Valuation Report',
        'Insurance Certificate',
        'Rates Notice',
        'Rental Appraisal',
        'Tenancy Agreement',
        'Rental Statement',

        # Other Supporting Documents
        'Gift Letter',
        'Guarantor Documents',
        'Superannuation Statement',
        'Utility Bills',
        'Miscellaneous or Unclassified',
        
        "Xero Reports",
        "MYOB Reports",
        "Identity Verification",
    }

    # Extract used categories from final_json
    used_categories = set()
    for entry in final_json:
        category = entry.get("broker_document_category")
        if category and category != "NA":
            used_categories.add(category)

    # Find the unused categories
    unused_categories = all_categories - used_categories
    return unused_categories


if __name__ == "__main__":
    cats = find_unused_broker_doc_categories({})
    print(cats)

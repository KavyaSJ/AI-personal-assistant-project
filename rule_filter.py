# rule_filter.py

def classify_email(sender, subject, headers):
    """ Part of Email Pipeline. This section builds everything related
    to emails: reading them, filtering them, running them through the AI,
    writing draft replies, and remembering what has already been done"""
    
    sender_lower = (sender or "").lower()
    subject_lower = (subject or "").lower()
    headers = headers or {}

    has_list_unsubscribe = False
    for key in headers:
        if str(key).lower() == "list-unsubscribe":
            has_list_unsubscribe = True
            break

    # Catch automated system emails that should not go to the AI.
    if (
        "noreply" in sender_lower
        or "no-reply" in sender_lower
        or "do-not-reply" in sender_lower
        or "mailer-daemon" in sender_lower
    ):
        return "automated"

    # Catch newsletters, promos, and mailing-list style emails.
    elif (
        has_list_unsubscribe
        or "newsletter" in subject_lower
        or "unsubscribe" in subject_lower
        or "promo" in subject_lower
        or "offer" in subject_lower
        or "deal" in subject_lower
        or "% off" in subject_lower
        or "sale" in subject_lower
    ):
        return "promotional"

    # Catch purchase and payment related emails.
    elif (
        "receipt" in subject_lower
        or "invoice" in subject_lower
        or "order #" in subject_lower
        or "order confirmation" in subject_lower
        or "payment received" in subject_lower
    ):
        return "transactional"

    # Anything else becomes a candidate for AI processing.
    else:
        return "candidate"


if __name__ == "__main__":
    tests = [
        {
            "sender": "noreply@bank.com",
            "subject": "Security Alert",
            "headers": {},
            "expected": "automated",
        },
        {
            "sender": "mailer-daemon@mailserver.com",
            "subject": "Delivery Status Notification",
            "headers": {},
            "expected": "automated",
        },
        {
            "sender": "news@shop.com",
            "subject": "Big Summer Sale - 40% Off",
            "headers": {},
            "expected": "promotional",
        },
        {
            "sender": "offers@store.com",
            "subject": "Weekly Update",
            "headers": {"List-Unsubscribe": "<mailto:unsubscribe@store.com>"},
            "expected": "promotional",
        },
        {
            "sender": "orders@amazon.com",
            "subject": "Order Confirmation #12345",
            "headers": {},
            "expected": "transactional",
        },
        {
            "sender": "professor@university.edu",
            "subject": "Can we meet tomorrow about your project?",
            "headers": {},
            "expected": "candidate",
        },
    ]

    for i, test in enumerate(tests, 1):
        result = classify_email(test["sender"], test["subject"], test["headers"])
        print(
            f"Test {i}: expected={test['expected']}, got={result}, pass={result == test['expected']}"
        )
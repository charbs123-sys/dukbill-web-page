import os
import smtplib
from email.message import EmailMessage
from datetime import date

PRIMARY = "#16a085"  # Accent color you can tweak
BG_BADGE = "#e6f7f3"
TEXT_DARK = "#0f172a"

def dukbill_style_html(
    broker_name: str,
    broker_email: str,
    client_first_name: str,
    msg_contents: str,
    cta_url: str | None = "https://dukbillapp.com/",
    headline: str | None = None,  # optional override for the header line
    today_str: str | None = None,  # for testing/preview override
) -> str:
    """
    Generates a Dukbill-style HTML email with simplified layout and the new color scheme.
    - Primary accent (CTA): #ff6b15
    - Background: #f4f6f8
    - Card text: #0f172a / #334155
    - Subtle grays: #6b7280, #9ca3af, divider #e5e7eb
    """
    # Safe escape for HTML
    def esc(s: str) -> str:
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    today = today_str or date.today().strftime("%Y-%m-%d")
    year = date.today().strftime("%Y")

    final_headline = (
        headline
        if headline
        else f"{esc(broker_name)} is trying to get in contact with you"
    )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="x-apple-disable-message-reformatting">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Broker Message</title>
</head>
<body style="margin:0;padding:0;background-color:#f4f6f8;">
  <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="background-color:#f4f6f8;">
    <tr>
      <td align="center" style="padding:36px 16px;">
        <table role="presentation" cellpadding="0" cellspacing="0" width="600" style="width:600px;max-width:100%;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#111827;">
          <tr>
            <td style="padding-bottom:12px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td align="left">
                    <span style="display:inline-block;font-weight:700;font-size:18px;letter-spacing:0.2px;color:#0f172a;">{final_headline}</span>
                  </td>
                  <td align="right">
                    <span style="display:inline-block;font-size:12px;color:#6b7280;">{today}</span>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <tr>
            <td>
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#ffffff;border-radius:14px;padding:28px;box-shadow:0 1px 2px rgba(16,24,40,0.04),0 4px 12px rgba(16,24,40,0.06);">

                <tr>
                  <td style="padding:12px 0 4px 0;">
                    <p style="margin:0;font-size:15px;line-height:1.7;color:#334155;">Hi {esc(client_first_name)},</p>
                  </td>
                </tr>

                <tr>
                  <td style="padding:8px 0 12px 0;">
                    <p style="margin:0;font-size:15px;line-height:1.7;color:#334155;">{esc(msg_contents).replace('\\n','<br>')}</p>
                  </td>
                </tr>

                <tr>
                  <td style="padding:12px 0 0 0;">
                    <p style="margin:0;font-size:15px;line-height:1.7;color:#334155;">
                      Best regards,<br>
                      <strong>{esc(broker_name)}</strong><br>
                      <span style="color:#6b7280;">{esc(broker_email)}</span>
                    </p>
                  </td>
                </tr>

                {f"""<tr>
                  <td style="padding-top:20px;">
                    <a href="{esc(cta_url)}" style="display:inline-block;background-color:#ff6b15;color:#ffffff;text-decoration:none;padding:12px 18px;border-radius:10px;font-weight:600;font-size:14px;">View Details</a>
                  </td>
                </tr>""" if cta_url else ""}

                <tr>
                  <td style="padding-top:20px;">
                    <hr style="border:0;border-top:1px solid #e5e7eb;margin:0;">
                  </td>
                </tr>

                <tr>
                  <td style="padding-top:12px;">
                    <p style="margin:0;font-size:12px;line-height:1.6;color:#6b7280;">If you have any questions, reply to this email.</p>
                  </td>
                </tr>

              </table>
            </td>
          </tr>

          <tr>
            <td align="center" style="padding:20px 8px 0 8px;">
              <p style="margin:0;font-size:12px;color:#9ca3af;line-height:1.6;">© {year} Your Company. All rights reserved.</p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""
    return html

def send_broker_to_client(
    broker_name: str,
    broker_email: str,
    client_first_name: str,
    client_email: str,
    msg_contents: str,
    msg_type: str,
    subject: str | None = None,
    cta_url: str | None = None,
) -> bool:
    from_email = os.getenv("EMAIL_ADDRESS")
    from_password = os.getenv("EMAIL_PASSWORD")
    if not from_email or not from_password:
        print("Missing EMAIL_ADDRESS or EMAIL_PASSWORD in environment.")
        return False

    msg = EmailMessage()
    msg["From"] = f"{broker_name} <{from_email}>"
    msg["Reply-To"] = broker_email  # so replies go to the broker
    msg["To"] = client_email
    msg["Subject"] = subject or f"Message from {broker_name}"

    # Plain-text fallback (for older clients)
    plain = (
        f"Hi {client_first_name},\n\n"
        f"{msg_contents}\n\n"
        f"Best regards,\n{broker_name}\n{broker_email}\n"
    )
    msg.set_content(plain)

    if msg_type == "onboarding":
        # HTML version for onboarding
        html = dukbill_style_html(
            broker_name=broker_name,
            broker_email=broker_email,
            client_first_name=client_first_name,
            msg_contents=msg_contents,
            cta_url=cta_url or "https://dukbillapp.com/onboarding",
            today_str=date.today().strftime("%Y-%m-%d"),
        )
    elif msg_type == "verification_success":
        # HTML version for verification success
        html = dukbill_verification_success_html(
            broker_name=broker_name,
            client_first_name=client_first_name,
            cta_url=cta_url or "https://dukbillapp.com/dashboard",
            today_str=date.today().strftime("%Y-%m-%d"),
        )
    msg.add_alternative(html, subtype="html")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(from_email, from_password)
            server.send_message(msg)
        print(f"Email successfully sent to {client_email}")
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False
    
  

def dukbill_verification_success_html(
    broker_name: str,
    client_first_name: str,
    cta_url: str = "https://dukbillapp.com/dashboard",
    today_str: str | None = None,
) -> str:
    """
    Generates a Dukbill-style HTML email specifically for successful document verification.
    """
    # Safe escape for HTML
    def esc(s: str) -> str:
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    today = today_str or date.today().strftime("%Y-%m-%d")
    year = date.today().strftime("%Y")

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="x-apple-disable-message-reformatting">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Verification Successful</title>
</head>
<body style="margin:0;padding:0;background-color:#f4f6f8;">
  <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="background-color:#f4f6f8;">
    <tr>
      <td align="center" style="padding:36px 16px;">
        <table role="presentation" cellpadding="0" cellspacing="0" width="600" style="width:600px;max-width:100%;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:#111827;">
          <tr>
            <td style="padding-bottom:12px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td align="left">
                    <span style="display:inline-block;font-weight:700;font-size:18px;letter-spacing:0.2px;color:#0f172a;">Verification Successful</span>
                  </td>
                  <td align="right">
                    <span style="display:inline-block;font-size:12px;color:#6b7280;">{today}</span>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <tr>
            <td>
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#ffffff;border-radius:14px;padding:28px;box-shadow:0 1px 2px rgba(16,24,40,0.04),0 4px 12px rgba(16,24,40,0.06);">

                <tr>
                  <td style="padding:12px 0 4px 0;">
                    <p style="margin:0;font-size:15px;line-height:1.7;color:#334155;">Hi {esc(client_first_name)},</p>
                  </td>
                </tr>

                <tr>
                  <td style="padding:8px 0 12px 0;">
                    <p style="margin:0;font-size:15px;line-height:1.7;color:#334155;">
                      Great news! <strong>{esc(broker_name)}</strong> has successfully verified your documents.
                    </p>
                    <p style="margin:16px 0 0 0;font-size:15px;line-height:1.7;color:#334155;">
                      Please check your verification status on your dashboard to proceed with the next steps.
                    </p>
                  </td>
                </tr>

                <tr>
                  <td style="padding-top:24px;">
                    <a href="{esc(cta_url)}" style="display:inline-block;background-color:#ff6b15;color:#ffffff;text-decoration:none;padding:12px 24px;border-radius:8px;font-weight:600;font-size:14px;box-shadow:0 2px 4px rgba(255,107,21,0.2);">Check Verification Status</a>
                  </td>
                </tr>

                <tr>
                  <td style="padding:24px 0 0 0;">
                    <p style="margin:0;font-size:15px;line-height:1.7;color:#334155;">
                      Kind regards,<br>
                      <strong>The Dukbill Team</strong>
                    </p>
                  </td>
                </tr>

                <tr>
                  <td style="padding-top:20px;">
                    <hr style="border:0;border-top:1px solid #e5e7eb;margin:0;">
                  </td>
                </tr>

                <tr>
                  <td style="padding-top:12px;">
                    <p style="margin:0;font-size:12px;line-height:1.6;color:#6b7280;">If you didn't request this verification, please ignore this email.</p>
                  </td>
                </tr>

              </table>
            </td>
          </tr>

          <tr>
            <td align="center" style="padding:20px 8px 0 8px;">
              <p style="margin:0;font-size:12px;color:#9ca3af;line-height:1.6;">© {year} Dukbill. All rights reserved.</p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""
    return html
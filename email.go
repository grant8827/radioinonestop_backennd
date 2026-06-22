package main

import (
	"crypto/tls"
	"fmt"
	"net/smtp"
	"os"
	"time"
)

const (
	mailtrapHost  = "live.smtp.mailtrap.io"
	mailtrapPort  = "587"
	mailtrapUser  = "apismtp@mailtrap.io"
	emailFromAddr = "noreply@radioinonestop.com"
	emailFromName = "Radio In One Stop"
)

func sendMail(to, subject, htmlBody string) error {
	token := os.Getenv("MAILTRAP_API_TOKEN")
	if token == "" {
		return fmt.Errorf("MAILTRAP_API_TOKEN not set")
	}
	client, err := smtp.Dial(mailtrapHost + ":" + mailtrapPort)
	if err != nil {
		return fmt.Errorf("smtp dial: %w", err)
	}
	defer client.Close()
	if err = client.StartTLS(&tls.Config{ServerName: mailtrapHost}); err != nil {
		return fmt.Errorf("starttls: %w", err)
	}
	auth := smtp.PlainAuth("", mailtrapUser, token, mailtrapHost)
	if err = client.Auth(auth); err != nil {
		return fmt.Errorf("smtp auth: %w", err)
	}
	if err = client.Mail(emailFromAddr); err != nil {
		return fmt.Errorf("smtp mail from: %w", err)
	}
	if err = client.Rcpt(to); err != nil {
		return fmt.Errorf("smtp rcpt: %w", err)
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp data: %w", err)
	}
	msg := fmt.Sprintf(
		"From: %s <%s>\r\nTo: %s\r\nSubject: %s\r\nMIME-version: 1.0\r\nContent-Type: text/html; charset=\"UTF-8\"\r\n\r\n%s",
		emailFromName, emailFromAddr, to, subject, htmlBody,
	)
	if _, err = fmt.Fprint(w, msg); err != nil {
		return fmt.Errorf("smtp write: %w", err)
	}
	return w.Close()
}

func sendOTPEmail(to, firstName, otp string) error {
	return sendMail(to, "Your Radio In One Stop verification code", emailOTPHTML(firstName, otp))
}

func sendWelcomeEmail(to, firstName string) error {
	return sendMail(to, "Welcome to Radio In One Stop!", emailWelcomeHTML(firstName))
}

func sendPasswordResetEmail(to, firstName, resetLink string) error {
	return sendMail(to, "Reset your Radio In One Stop password", emailPasswordResetHTML(firstName, resetLink))
}

func sendSubscriptionConfirmedEmail(to, firstName, plan string) error {
	return sendMail(to, "Subscription confirmed — you're live on Radio In One Stop!", emailSubscriptionConfirmedHTML(firstName, plan))
}

func sendSubscriptionFailedEmail(to, firstName string) error {
	return sendMail(to, "Action needed: payment failed on Radio In One Stop", emailSubscriptionFailedHTML(firstName))
}

// ── HTML templates ────────────────────────────────────────────────────────────

func emailBase(title, preheader, content string) string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>%s</title>
</head>
<body style="margin:0;padding:0;background-color:#0a0a12;font-family:Arial,sans-serif;">
<span style="display:none;max-height:0;overflow:hidden;">%s</span>
<table width="100%%" cellpadding="0" cellspacing="0" style="background:#0a0a12;padding:40px 20px;">
<tr><td align="center">
<table width="100%%" cellpadding="0" cellspacing="0" style="max-width:560px;">
  <tr><td style="background:linear-gradient(135deg,#4c1d95,#1e3a8a);border-radius:16px 16px 0 0;padding:32px;text-align:center;">
    <p style="margin:0 0 12px;font-size:32px;">📻</p>
    <h1 style="margin:0;color:white;font-size:22px;font-weight:bold;">Radio In One Stop</h1>
  </td></tr>
  <tr><td style="background:#0f0f1a;padding:40px 32px;border-left:1px solid rgba(255,255,255,0.08);border-right:1px solid rgba(255,255,255,0.08);">
    %s
  </td></tr>
  <tr><td style="background:#0a0a12;border:1px solid rgba(255,255,255,0.05);border-top:none;border-radius:0 0 16px 16px;padding:20px 32px;text-align:center;">
    <p style="margin:0;color:#6b7280;font-size:12px;">© %d Radio In One Stop</p>
  </td></tr>
</table>
</td></tr>
</table>
</body>
</html>`, title, preheader, content, time.Now().Year())
}

func emailOTPHTML(firstName, otp string) string {
	content := fmt.Sprintf(`
<h2 style="margin:0 0 8px;color:white;font-size:20px;font-weight:bold;">Verify your email</h2>
<p style="margin:0 0 24px;color:#9ca3af;font-size:15px;line-height:1.6;">Hi %s, use the code below to complete your registration. It expires in <strong style="color:white;">15 minutes</strong>.</p>
<div style="background:#1a1a2e;border:1px solid rgba(255,255,255,0.1);border-radius:12px;padding:28px;text-align:center;margin-bottom:24px;">
  <p style="margin:0 0 8px;color:#9ca3af;font-size:11px;text-transform:uppercase;letter-spacing:3px;">Verification Code</p>
  <p style="margin:0;color:white;font-size:44px;font-weight:bold;letter-spacing:10px;font-family:monospace;">%s</p>
</div>
<p style="margin:0;color:#6b7280;font-size:13px;">If you didn't create an account, you can safely ignore this email.</p>`, firstName, otp)
	return emailBase("Verify your email", "Your verification code is "+otp, content)
}

func emailWelcomeHTML(firstName string) string {
	content := fmt.Sprintf(`
<h2 style="margin:0 0 8px;color:white;font-size:20px;font-weight:bold;">Welcome aboard, %s! 🎙️</h2>
<p style="margin:0 0 24px;color:#9ca3af;font-size:15px;line-height:1.6;">Your email is verified and your Radio In One Stop account is ready. Start broadcasting to the world!</p>
<div style="text-align:center;margin:32px 0;">
  <a href="https://radioinonestop.com" style="background:linear-gradient(135deg,#dc2626,#b45309);color:white;text-decoration:none;padding:14px 32px;border-radius:10px;font-weight:bold;font-size:15px;">Go to your Studio →</a>
</div>
<p style="margin:0;color:#6b7280;font-size:13px;">Need help? Reach out to us anytime.</p>`, firstName)
	return emailBase("Welcome to Radio In One Stop!", "Your account is ready", content)
}

func emailPasswordResetHTML(firstName, resetLink string) string {
	content := fmt.Sprintf(`
<h2 style="margin:0 0 8px;color:white;font-size:20px;font-weight:bold;">Reset your password</h2>
<p style="margin:0 0 24px;color:#9ca3af;font-size:15px;line-height:1.6;">Hi %s, click the button below to set a new password. This link expires in <strong style="color:white;">1 hour</strong>.</p>
<div style="text-align:center;margin:32px 0;">
  <a href="%s" style="background:linear-gradient(135deg,#dc2626,#b45309);color:white;text-decoration:none;padding:14px 32px;border-radius:10px;font-weight:bold;font-size:15px;">Reset my password</a>
</div>
<p style="margin:0 0 12px;color:#6b7280;font-size:13px;">If the button doesn't work, copy and paste this link into your browser:</p>
<p style="margin:0;color:#9ca3af;font-size:12px;word-break:break-all;">%s</p>
<p style="margin:24px 0 0;color:#6b7280;font-size:13px;">If you didn't request a password reset, you can safely ignore this email.</p>`, firstName, resetLink, resetLink)
	return emailBase("Reset your password", "Reset your Radio In One Stop password", content)
}

func emailSubscriptionConfirmedHTML(firstName, plan string) string {
	content := fmt.Sprintf(`
<h2 style="margin:0 0 8px;color:white;font-size:20px;font-weight:bold;">Subscription confirmed! 🎉</h2>
<p style="margin:0 0 24px;color:#9ca3af;font-size:15px;line-height:1.6;">Hi %s, your <strong style="color:white;">%s</strong> plan is now active. Your station is live and ready for listeners!</p>
<div style="text-align:center;margin:32px 0;">
  <a href="https://radioinonestop.com" style="background:linear-gradient(135deg,#dc2626,#b45309);color:white;text-decoration:none;padding:14px 32px;border-radius:10px;font-weight:bold;font-size:15px;">Go to your Studio →</a>
</div>
<p style="margin:0;color:#6b7280;font-size:13px;">Thank you for subscribing to Radio In One Stop.</p>`, firstName, plan)
	return emailBase("Subscription confirmed!", "Your "+plan+" plan is now active", content)
}

func emailSubscriptionFailedHTML(firstName string) string {
	content := fmt.Sprintf(`
<h2 style="margin:0 0 8px;color:white;font-size:20px;font-weight:bold;">Payment failed ⚠️</h2>
<p style="margin:0 0 24px;color:#9ca3af;font-size:15px;line-height:1.6;">Hi %s, we were unable to process your subscription payment. Please update your payment method to keep your station active.</p>
<div style="text-align:center;margin:32px 0;">
  <a href="https://radioinonestop.com" style="background:linear-gradient(135deg,#dc2626,#b45309);color:white;text-decoration:none;padding:14px 32px;border-radius:10px;font-weight:bold;font-size:15px;">Update payment method →</a>
</div>
<p style="margin:0;color:#6b7280;font-size:13px;">If you've already resolved this, you can ignore this email.</p>`, firstName)
	return emailBase("Payment failed — action needed", "We couldn't process your payment", content)
}

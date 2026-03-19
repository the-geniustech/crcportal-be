# Backend Mongoose Schema (Generated From Frontend)

This folder contains Mongoose model definitions inferred from the frontend codebase.

## Models

- `Profile` (maps `profiles` usage)
- `Group` (maps `groups` usage)
- `GroupMembership` (inferred from group member roles/screens)
- `Contribution` (inferred from contribution tracking UI)
- `BankAccount` (maps `banking_details` usage)
- `WithdrawalRequest` (maps `withdrawal_requests` usage)
- `Meeting`, `MeetingAgendaItem`, `MeetingMinutes`, `MeetingAttendance` (maps `meetings`, `meeting_agenda_items`, `meeting_minutes` usage)
- `LoanApplication` (inferred from loan application/review UI)
- `LoanGuarantor` (maps `loan_guarantors` usage)
- `GuarantorNotification` (maps `guarantor_notifications` usage)
- `RecurringPayment` (maps `recurring_payments` usage)
- `Transaction` (inferred from payments UI + paystack flow)

## Notes

- Field names are now `camelCase` throughout the backend.
- Some entities (like transactions/loan applications) are mocked in the UI, so the schema is inferred from the TypeScript interfaces and payload shapes in the repo.

## Usage

```js
import { connectMongo } from "crc-backend-schema/db";
import { ProfileModel } from "crc-backend-schema/models";

await connectMongo({ mongoUri: process.env.MONGO_URI });
const profile = await ProfileModel.findOne({ email: "a@b.com" });
```

## API (WIP)

Auth

- `POST /api/v1/auth/signup` (email/phone + password) → sends verification
- `POST /api/v1/auth/login` (email/phone + password) → returns `{ accessToken, refreshToken }` after verification
- `POST /api/v1/auth/resend-verification` (email/phone)
- `GET /api/v1/auth/verify-email?token=... (&autoLogin=true optional)`
- `POST /api/v1/auth/verify-phone` (phone + otp, autoLogin optional)
- `POST /api/v1/auth/forgot-password` (email/phone)
- `POST /api/v1/auth/reset-password` (token+password OR phone+otp+password, autoLogin optional)
- `PATCH /api/v1/auth/change-password` (Bearer `accessToken`, currentPassword + newPassword)
- `POST /api/v1/auth/refresh` (send `refreshToken`) → rotates + returns new `{ accessToken, refreshToken }`
- `POST /api/v1/auth/logout` (send `refreshToken`) → revokes refresh token
- `GET /api/v1/auth/me` (Bearer `accessToken`)

Users

- `GET /api/v1/users/me` (Bearer `accessToken`)
- `PATCH /api/v1/users/me` (Bearer `accessToken`)
- `PATCH /api/v1/users/me` also accepts `multipart/form-data` with `avatar` (image) and profile fields like `fullName`, `phone`.

Admin

- `GET /api/v1/users` (Bearer `accessToken`, role `admin`)
- `PATCH /api/v1/users/:id/role` (Bearer `accessToken`, role `admin`)

## Environment

Core

- `MONGO_URI` (required)
- `PUBLIC_BASE_URL` (optional; used to build email verification links)
- `PASSWORD_RESET_URL_BASE` (optional; base URL used in password reset emails; defaults to `PUBLIC_BASE_URL`)
- `APP_NAME` (optional; used as User-Agent)

JWT

- `JWT_ACCESS_SECRET` (required; falls back to `JWT_SECRET`)
- `JWT_REFRESH_SECRET` (required)
- `JWT_ACCESS_EXPIRES_IN` (optional, default `15m`)
- `JWT_REFRESH_EXPIRES_IN` (optional, default `30d`)

Email (Resend)

- `RESEND_API_KEY` (required for email verification)
- `RESEND_FROM` (required; e.g. `Coop <noreply@yourdomain.com>`)

PDF (Receipt)

- Receipt PDFs are generated with `pdfkit` on the backend.

SMS (Termii)

- `TERMII_API_KEY` (required for phone OTP)
- `TERMII_SENDER_ID` (required)
- `TERMII_BASE_URL` (optional, default `https://api.ng.termii.com`)
- `TERMII_SEND_PATH` (optional, default `/api/sms/send`)
- `TERMII_CHANNEL` (optional, default `generic`)
  Cloudinary

- `CLOUDINARY_CLOUD_NAME` (required for uploads)
- `CLOUDINARY_API_KEY` (required for uploads)
- `CLOUDINARY_API_SECRET` (required for uploads)

Server

- `PORT` (optional, default `4000`)
- `CORS_ORIGIN` (optional, comma-separated)

Payments (Paystack)

- `PAYSTACK_SECRET_KEY` (required; used for initialize/verify + webhook signature validation)
- `PAYSTACK_CALLBACK_URL` (optional; fallback callback URL passed to Paystack initialization)

## Run

- `npm install`
- `npm run dev`

## Notes - 2

- Basic in-memory rate limiting is enabled on `POST /api/v1/auth/resend-verification`, `POST /api/v1/auth/verify-phone`, `POST /api/v1/auth/forgot-password`, and `POST /api/v1/auth/reset-password`.

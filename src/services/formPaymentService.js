import { FormPaymentModel } from "../models/FormPayment.js";
import { GroupModel } from "../models/Group.js";
import { ProfileModel } from "../models/Profile.js";
import { TransactionModel } from "../models/Transaction.js";
import { UserModel } from "../models/User.js";
import { getLoanFacility } from "../utils/loanPolicy.js";

export const BSS_FORM_PAYMENT_TYPES = [
  "bridging_loan",
  "soft_loan",
  "special_loan",
];

export const FORM_PAYMENT_CONFIG = {
  membership_registration: {
    formType: "membership_registration",
    formCategory: "membership",
    formLabel: "Membership Registration Form",
    amount: 2000,
  },
  revolving_loan: {
    formType: "revolving_loan",
    formCategory: "loan",
    formLabel: "Revolving Loan Form",
    amount: 1000,
  },
  bridging_loan: {
    formType: "bridging_loan",
    formCategory: "loan",
    formLabel: "BSS Loan Form",
    amount: 2000,
  },
  soft_loan: {
    formType: "soft_loan",
    formCategory: "loan",
    formLabel: "BSS Loan Form",
    amount: 2000,
  },
  special_loan: {
    formType: "special_loan",
    formCategory: "loan",
    formLabel: "BSS Loan Form",
    amount: 2000,
  },
};

const LOAN_TYPE_TO_FORM_TYPE = {
  revolving: "revolving_loan",
  bridging: "bridging_loan",
  soft: "soft_loan",
  special: "special_loan",
};

function toPlain(value) {
  if (!value) return value;
  if (typeof value.toObject === "function") {
    return value.toObject({ depopulate: true, versionKey: false });
  }
  return value;
}

function sanitizeDetails(value) {
  if (!value) return {};
  return JSON.parse(JSON.stringify(value));
}

function buildFormPaymentTransactionReference(payment) {
  return `CRC-FORM-${String(payment?._id || "").toUpperCase()}`;
}

export function isBssFormPaymentType(formType) {
  return BSS_FORM_PAYMENT_TYPES.includes(String(formType || ""));
}

export function resolveFormPaymentDisplayLabel(paymentOrType, fallback = null) {
  const formType =
    typeof paymentOrType === "string"
      ? paymentOrType
      : paymentOrType?.formType;
  if (isBssFormPaymentType(formType)) return "BSS Loan Form";
  return (
    (typeof paymentOrType === "object" ? paymentOrType?.formLabel : null) ||
    fallback ||
    FORM_PAYMENT_CONFIG[formType]?.formLabel ||
    "Form Payment"
  );
}

function mapFormPaymentStatusToTransactionStatus(status) {
  if (status === "paid") return "success";
  if (status === "defaulted") return "failed";
  return "pending";
}

function resolveTransactionDate(payment) {
  return (
    payment.reviewedAt ||
    payment.submittedAt ||
    payment.createdAt ||
    new Date()
  );
}

function resolveDate(value, fallback = new Date()) {
  if (!value) return fallback;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? fallback : parsed;
}

async function buildMemberSnapshot(profileId) {
  if (!profileId) {
    return {
      profile: null,
      user: null,
      userAccountId: null,
      memberName: null,
      memberEmail: null,
      memberPhone: null,
    };
  }

  const [profile, user] = await Promise.all([
    ProfileModel.findById(profileId)
      .select(
        [
          "fullName",
          "email",
          "phone",
          "dateOfBirth",
          "address",
          "city",
          "state",
          "occupation",
          "employer",
          "nextOfKinName",
          "nextOfKinPhone",
          "nextOfKinRelationship",
          "membershipStatus",
        ].join(" "),
      )
      .lean(),
    UserModel.findOne({ profileId }).select("_id email phone").lean(),
  ]);

  return {
    profile,
    user,
    userAccountId: user?._id ?? null,
    memberName: profile?.fullName || null,
    memberEmail: profile?.email || user?.email || null,
    memberPhone: profile?.phone || user?.phone || null,
  };
}

async function resolveGroup(groupOrId) {
  if (!groupOrId) return null;
  if (typeof groupOrId === "object" && groupOrId.groupName) return groupOrId;
  return GroupModel.findById(groupOrId).select("groupName groupNumber").lean();
}

function buildMembershipDetails(membership, group, snapshot) {
  const plain = toPlain(membership) || {};
  const groupPlain = toPlain(group) || {};
  const profile = snapshot?.profile || {};

  return sanitizeDetails({
    membershipId: plain._id,
    membershipStatus: plain.status,
    requestedAt: plain.requestedAt,
    joinedAt: plain.joinedAt,
    role: plain.role,
    memberSerial: plain.memberSerial,
    memberNumber: plain.memberNumber,
    group: {
      id: groupPlain._id ?? plain.groupId ?? null,
      name: groupPlain.groupName ?? null,
      number: groupPlain.groupNumber ?? null,
    },
    member: {
      fullName: profile.fullName ?? null,
      email: profile.email ?? snapshot?.user?.email ?? null,
      phone: profile.phone ?? snapshot?.user?.phone ?? null,
      dateOfBirth: profile.dateOfBirth ?? null,
      address: profile.address ?? null,
      city: profile.city ?? null,
      state: profile.state ?? null,
      occupation: profile.occupation ?? null,
      employer: profile.employer ?? null,
      nextOfKinName: profile.nextOfKinName ?? null,
      nextOfKinPhone: profile.nextOfKinPhone ?? null,
      nextOfKinRelationship: profile.nextOfKinRelationship ?? null,
      profileStatus: profile.membershipStatus ?? null,
    },
  });
}

function buildLoanDetails(application, group) {
  const plain = toPlain(application) || {};
  const groupPlain = toPlain(group) || {};
  const facility = getLoanFacility(plain.loanType);

  return sanitizeDetails({
    applicationId: plain._id,
    loanCode: plain.loanCode,
    loanNumber: plain.loanNumber,
    loanType: plain.loanType,
    loanLabel: facility?.label || plain.loanType || null,
    loanAmount: plain.loanAmount,
    loanPurpose: plain.loanPurpose,
    purposeDescription: plain.purposeDescription,
    repaymentPeriod: plain.repaymentPeriod,
    interestRate: plain.interestRate,
    interestRateType: plain.interestRateType,
    monthlyIncome: plain.monthlyIncome,
    status: plain.status,
    group: {
      id: groupPlain._id ?? plain.groupId ?? null,
      name: groupPlain.groupName ?? plain.groupName ?? null,
      number: groupPlain.groupNumber ?? null,
    },
    disbursementAccount: {
      bankName: plain.disbursementBankName,
      accountNumber: plain.disbursementAccountNumber,
      accountName: plain.disbursementAccountName,
    },
    documents: plain.documents || [],
    guarantors: plain.guarantors || [],
  });
}

export function resolveFormPaymentConfig(formType) {
  return FORM_PAYMENT_CONFIG[formType] || null;
}

export function resolveLoanFormPaymentConfig(loanType) {
  const formType = LOAN_TYPE_TO_FORM_TYPE[String(loanType || "").toLowerCase()];
  return formType ? resolveFormPaymentConfig(formType) : null;
}

export async function upsertMembershipFormPayment({
  membership,
  group,
  syncTransaction = true,
}) {
  const plainMembership = toPlain(membership);
  if (!plainMembership?._id || !plainMembership.userId) return null;

  const config = resolveFormPaymentConfig("membership_registration");
  const resolvedGroup = await resolveGroup(group || plainMembership.groupId);
  const snapshot = await buildMemberSnapshot(plainMembership.userId);
  const submittedAt = resolveDate(
    plainMembership.requestedAt || plainMembership.createdAt,
  );

  const payment = await FormPaymentModel.findOneAndUpdate(
    {
      sourceModel: "GroupMembership",
      sourceId: plainMembership._id,
      formType: config.formType,
    },
    {
      $set: {
        userId: plainMembership.userId,
        userAccountId: snapshot.userAccountId,
        groupId: plainMembership.groupId || resolvedGroup?._id || null,
        groupName: resolvedGroup?.groupName || null,
        memberName: snapshot.memberName,
        memberEmail: snapshot.memberEmail,
        memberPhone: snapshot.memberPhone,
        formCategory: config.formCategory,
        formLabel: config.formLabel,
        amount: config.amount,
        currency: "NGN",
        sourceReference: resolvedGroup?.groupName || "Membership request",
        submittedAt,
        formDetails: buildMembershipDetails(
          plainMembership,
          resolvedGroup,
          snapshot,
        ),
      },
      $setOnInsert: {
        paymentStatus: "pending",
      },
    },
    {
      upsert: true,
      new: true,
      runValidators: true,
      setDefaultsOnInsert: true,
    },
  );

  if (syncTransaction) {
    await syncFormPaymentTransaction(payment);
  }
  return payment;
}

export async function upsertLoanFormPayment({
  application,
  group,
  syncTransaction = true,
}) {
  const plainApplication = toPlain(application);
  if (
    !plainApplication?._id ||
    !plainApplication.userId ||
    plainApplication.status === "draft"
  ) {
    return null;
  }

  const config = resolveLoanFormPaymentConfig(plainApplication.loanType);
  if (!config) return null;

  const resolvedGroup = await resolveGroup(group || plainApplication.groupId);
  const snapshot = await buildMemberSnapshot(plainApplication.userId);
  const submittedAt = resolveDate(
    plainApplication.updatedAt || plainApplication.createdAt,
  );

  const payment = await FormPaymentModel.findOneAndUpdate(
    {
      sourceModel: "LoanApplication",
      sourceId: plainApplication._id,
      formType: config.formType,
    },
    {
      $set: {
        userId: plainApplication.userId,
        userAccountId: snapshot.userAccountId,
        groupId: plainApplication.groupId || resolvedGroup?._id || null,
        groupName: resolvedGroup?.groupName || plainApplication.groupName || null,
        memberName: snapshot.memberName,
        memberEmail: snapshot.memberEmail,
        memberPhone: snapshot.memberPhone,
        formCategory: config.formCategory,
        formLabel: config.formLabel,
        amount: config.amount,
        currency: "NGN",
        sourceReference: plainApplication.loanCode || null,
        submittedAt,
        formDetails: buildLoanDetails(plainApplication, resolvedGroup),
      },
      $setOnInsert: {
        paymentStatus: "pending",
      },
    },
    {
      upsert: true,
      new: true,
      runValidators: true,
      setDefaultsOnInsert: true,
    },
  );

  if (syncTransaction) {
    await syncFormPaymentTransaction(payment);
  }
  return payment;
}

export async function syncFormPaymentTransaction(
  paymentOrId,
  { actorProfileId = null, channel = "form_payment_review" } = {},
) {
  const payment =
    typeof paymentOrId === "string" || paymentOrId?._bsontype === "ObjectId"
      ? await FormPaymentModel.findById(paymentOrId)
      : paymentOrId;

  if (!payment?._id || !payment.userId) return null;

  const reference =
    payment.transactionReference || buildFormPaymentTransactionReference(payment);
  const txStatus = mapFormPaymentStatusToTransactionStatus(payment.paymentStatus);
  const txDate = resolveTransactionDate(payment);
  const description = `${payment.formLabel} payment - ${
    payment.memberName || "Member"
  }`;
  const metadata = {
    paymentType: "form_payment",
    formPaymentId: payment._id,
    formType: payment.formType,
    formCategory: payment.formCategory,
    formLabel: payment.formLabel,
    paymentStatus: payment.paymentStatus,
    sourceModel: payment.sourceModel,
    sourceId: payment.sourceId,
    sourceReference: payment.sourceReference || null,
    submittedAt: payment.submittedAt || null,
    reviewedAt: payment.reviewedAt || null,
    reviewedBy: payment.reviewedBy || null,
    syncedBy: actorProfileId || null,
    syncedAt: new Date(),
  };

  const transaction = await TransactionModel.findOneAndUpdate(
    {
      $or: [
        { "metadata.formPaymentId": payment._id },
        { reference },
      ],
    },
    {
      $set: {
        userId: payment.userId,
        reference,
        amount: payment.amount,
        type: "form_payment",
        status: txStatus,
        description,
        channel,
        groupId: payment.groupId || null,
        groupName: payment.groupName || null,
        loanId:
          payment.sourceModel === "LoanApplication" ? payment.sourceId : null,
        loanName:
          payment.sourceModel === "LoanApplication"
            ? payment.sourceReference || payment.formLabel
            : null,
        metadata,
        gateway: "manual",
        date: txDate,
      },
    },
    {
      upsert: true,
      new: true,
      runValidators: true,
      setDefaultsOnInsert: true,
    },
  );

  const needsPaymentPatch =
    String(payment.transactionId || "") !== String(transaction._id) ||
    payment.transactionReference !== transaction.reference;

  if (needsPaymentPatch) {
    await FormPaymentModel.updateOne(
      { _id: payment._id },
      {
        $set: {
          transactionId: transaction._id,
          transactionReference: transaction.reference,
        },
      },
    );
    payment.transactionId = transaction._id;
    payment.transactionReference = transaction.reference;
  }

  return transaction;
}

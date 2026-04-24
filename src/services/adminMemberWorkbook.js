import ExcelJS from "exceljs";

export async function generateAdminMembersWorkbookBuffer({ rows }) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = "CRC Admin";
  workbook.lastModifiedBy = "CRC Admin";
  workbook.created = new Date();
  workbook.modified = new Date();

  const worksheet = workbook.addWorksheet("Members", {
    views: [{ state: "frozen", ySplit: 1 }],
  });

  worksheet.columns = [
    { header: "Member Serial", key: "memberSerial", width: 20 },
    { header: "Full Name", key: "fullName", width: 28 },
    { header: "Email", key: "email", width: 30 },
    { header: "Phone", key: "phone", width: 18 },
    { header: "Group", key: "groupName", width: 24 },
    { header: "Role", key: "role", width: 16 },
    { header: "Member Status", key: "memberStatus", width: 18 },
    { header: "Profile Status", key: "profileStatus", width: 18 },
    { header: "Joined", key: "joinedAt", width: 16 },
  ];

  rows.forEach((row) => {
    worksheet.addRow({
      memberSerial: row.memberSerial,
      fullName: row.fullName,
      email: row.email,
      phone: row.phone,
      groupName: row.groupName,
      role: row.role,
      memberStatus: row.memberStatus,
      profileStatus: row.profileStatus,
      joinedAt: row.joinedAt,
    });
  });

  const headerRow = worksheet.getRow(1);
  headerRow.font = { bold: true, color: { argb: "FFFFFFFF" } };
  headerRow.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FF0F766E" },
  };
  headerRow.alignment = { vertical: "middle", horizontal: "center" };
  headerRow.height = 22;

  worksheet.autoFilter = {
    from: "A1",
    to: "I1",
  };

  worksheet.eachRow((row, rowNumber) => {
    row.alignment = { vertical: "middle" };
    if (rowNumber > 1) {
      row.height = 20;
    }
  });

  const buffer = await workbook.xlsx.writeBuffer();
  return Buffer.from(buffer);
}

export const now = () => {
  const getCurrentDateTime = () => {
    const now = new Date();
    const options = { timeZone: "Asia/Ho_Chi_Minh" };

    const year = now.toLocaleString("en-GB", options).slice(6, 10);
    const month = (now.getMonth() + 1).toString().padStart(2, "0");
    const day = now.getDate().toString().padStart(2, "0");
    const hours = now.getHours().toString().padStart(2, "0");
    const minutes = now.getMinutes().toString().padStart(2, "0");
    const seconds = now.getSeconds().toString().padStart(2, "0");

    const formattedDateTime = `${year}${month}${day}_${hours}${minutes}${seconds}`;
    return formattedDateTime;
  };

  const currentDateTime = getCurrentDateTime();
  console.log(currentDateTime);
  return currentDateTime;
};

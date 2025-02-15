import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const nodemailer = require('nodemailer');

// Create a transporter object using Gmail's SMTP
const transporter = nodemailer.createTransport({
  service: 'Gmail',
  auth: {
    user: 'itpanabot@gmail.com',
    pass: 'tlklkfaxmfkkwnbe', // Use an app-specific password if enabled
  },
});

// Email data
const mailOptions = {
  from: 'itpanabot@gmail.com',
  to: ['ai.ahsanismail@gmail.com', 'logitixsolutions@gmail.com'],
  subject: 'Reported an issue in AWS TM',
  text: 'Please check something went wrong in AWS TM',
};

export const SendMail=(eventId,proxyAgent)=>{
    // Send email
transporter.sendMail({
  ...mailOptions,
  text:eventId?'Please check something went wrong in AWS TM with Event '+eventId+". Proxy Details:"+JSON.stringify(proxyAgent):mailOptions.text
}, (error, info) => {
    if (error) {
      console.error('Error:', error);
    } else {
      console.log('Email sent:', info.response);
    }
  });
  
}
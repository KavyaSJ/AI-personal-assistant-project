# AI-personal-assistant-project

## Overview

This project is an AI-powered personal assistant that helps users manage emails and documents efficiently. It automates email classification, generates intelligent replies, and extracts calendar events from documents like PDFs and Word files.

The system combines rule-based filtering with AI (Google Gemini) to provide accurate and efficient results while keeping the user in control.

🚀 Features

📧 Email Management

- Classifies emails into categories (personal, business, invitations, etc.)
- Filters out spam, promotional, and automated emails
- Generates professional email replies
- Supports draft refinement based on user feedback
- RSVP email generation

📄 Document Processing

- Extracts text from:
  - PDF files (pdfplumber)
  - Word documents (python-docx)
- Detects document type (e.g., course outline)
- Identifies important information

📅 Event Extraction & Calendar

- Extracts structured event details:
  - Title, Date, Time, Location
- Detects scheduling conflicts
- Suggests alternative time slots if there any conflicts
- Adds events to calendar after user confirmation

💬 Interactive Chat Interface

- Built using Streamlit
- Chat-based interaction with the assistant
- File upload support (PDF/DOCX)
- Email check trigger
- Draft approval workflow
- Event confirmation system
  
🏗️ Project Structure

ai-personal-assistant/
│
├── app.py                 # Main Streamlit app  
├── agent.py              # AI logic (Gemini integration)  
├── email_flow.py         # Email processing pipeline  
├── email_tool.py         # Gmail interaction functions  
├── calendar_tool.py      # Calendar integration  
├── pdf_tool.py           # PDF & DOCX text extraction  
├── rule_filter.py        # Rule-based email filtering  
├── memory.py             # Stores user decisions/history  
├── requirements.txt      # Dependencies  
├── .gitignore            # Ignored files  
└── README.md             # Project documentation  

⚙️ Technologies Used

- Python
- Streamlit (Frontend UI)
- Google Gemini API (google-generativeai)
- pdfplumber (PDF processing)
- python-docx (Word processing)
- dotenv (Environment variables)

🔐 Setup Instructions

1️⃣ Clone the Repository  
git clone https://github.com/KavyaSJ/AI-personal-assistant-project.git  
cd ai-personal-assistant-project

2️⃣ Install Dependencies  
pip install -r requirements.txt

3️⃣ Setup Environment Variables  
Create a .env file and add:  
GEMINI_API_KEY=your_api_key_here  

4️⃣ Run the Application  
streamlit run app.py

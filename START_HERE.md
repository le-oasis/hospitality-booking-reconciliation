# 🎯 START HERE - Interview Prep Package

**Mahmud's Complete Prep for The Agile Monkeys Interview**  
**Monday, October 28, 2025 | Interviewer: Amos Voron**

---

## 📦 YOUR FILES (In Priority Order)

### 1️⃣ **FINAL_INTERVIEW_PREP_SUMMARY.md** ⭐⭐⭐
**READ THIS FIRST**
- Your complete 3-day study plan (Friday → Monday)
- What to do each day
- The 30-minute assessment strategy
- Opening statement script
- Confidence boosters

**Time needed:** 15 minutes to read, reference all weekend

---

### 2️⃣ **hospitality_booking_reconciliation.tar.gz** ⭐⭐⭐
**YOUR DEMO PROJECT**
- Complete working Airflow pipeline
- GA4 → Salesforce reconciliation
- Realistic hospitality booking data
- Professional-grade code

**Time needed:** 1 hour to setup and run

**How to use:**
```bash
tar -xzf hospitality_booking_reconciliation.tar.gz
cd hospitality_booking_reconciliation
./start.sh
```

---

### 3️⃣ **INTERVIEW_CHEAT_SHEET.md** ⭐⭐⭐
**PRINT THIS OR KEEP OPEN DURING INTERVIEW**
- 30-second elevator pitch
- Q&A ready answers
- Key SQL queries
- Power statements
- Emergency phrases if you blank

**Time needed:** 30 minutes to memorize

**When to use:** Review Monday morning, keep nearby during call

---

### 4️⃣ **PROJECT_README.md** ⭐⭐
**TECHNICAL REFERENCE**
- Full project documentation
- Architecture diagrams
- Code explanations
- Interview talking points

**Time needed:** 20 minutes

**When to use:** After you've run the project, to understand deeper

---

## ⚡ QUICK START (If You Only Have 3 Hours)

### Hour 1: Run the Project
```bash
# Setup
tar -xzf hospitality_booking_reconciliation.tar.gz
cd hospitality_booking_reconciliation
./start.sh

# Generate data (after Airflow starts)
docker-compose exec airflow-scheduler bash
mkdir -p /opt/airflow/data
python /opt/airflow/dags/generate_data.py
exit

# Open browser: http://localhost:8080
# Login: airflow / airflow
# Trigger the DAG and watch it run
```

### Hour 2: Study the Cheat Sheet
- Open `INTERVIEW_CHEAT_SHEET.md`
- Memorize the 3 scenario answers
- Practice the 30-second pitch out loud
- Note the SQL reconciliation query

### Hour 3: Practice Demo
- Run the pipeline 2-3 times
- Look at the reconciliation report in Airflow logs
- Practice explaining: "This pipeline extracts from GA4 and Salesforce, runs quality checks, matches bookings, and categorizes discrepancies..."

---

## 📅 RECOMMENDED TIMELINE

### **Friday Night** (3 hours)
1. Read `FINAL_INTERVIEW_PREP_SUMMARY.md` (15 min)
2. Setup and run project (1 hour)
3. Study `INTERVIEW_CHEAT_SHEET.md` (45 min)
4. Practice 3-minute demo speech (1 hour)

### **Saturday** (2 hours)
1. Mock interview with yourself (1 hour)
2. Review Find.co code snippets I sent earlier (30 min)
3. Prepare 5 questions for Amos (30 min)

### **Sunday** (1 hour)
1. Quick review of key concepts (30 min)
2. Run project one more time (30 min)

### **Monday Morning** (1 hour before interview)
1. Start Airflow and generate fresh data (10 min)
2. Review `INTERVIEW_CHEAT_SHEET.md` (20 min)
3. Practice opening statement (10 min)
4. Deep breaths and confidence boost (20 min)

---

## 🎯 THE 30-MINUTE ASSESSMENT

**What Amos Will Test:**
1. Can you think through data problems systematically?
2. Do you know SQL and pipeline architecture?
3. Can you explain complex issues clearly?
4. Have you done this before (or just talking theory)?

**Your Strategy:**
1. Ask clarifying questions first
2. Outline approach before diving into details
3. Write/describe the SQL query
4. Reference your demo project
5. Connect to Find.co experience

**If he asks "Can you show me?"**
✅ Share screen → Airflow UI → Show the DAG → Show reconciliation report

---

## 🔥 KEY NUMBERS TO REMEMBER

From your demo project:
- **500** base bookings generated
- **485** GA4 events (95% capture rate)
- **472** Salesforce opportunities (92%)
- **85%** match rate (healthy for hospitality)
- **73%** GCLID attribution coverage
- **10** test bookings (GA4 only)
- **30** phone bookings (SF only)

From your Find.co experience:
- **22** Airflow DAGs
- **59** dbt models
- **10** GA4 brand implementations
- **15+** data sources integrated
- **30%** improvement in customer retention

---

## 💡 IF YOU HAVE LIMITED TIME

**Minimum Viable Prep (1 hour):**
1. Read `INTERVIEW_CHEAT_SHEET.md` (30 min)
2. Memorize the 3 scenario answers (30 min)

**Good Prep (3 hours):**
1. Run the project (1 hour)
2. Study cheat sheet (1 hour)
3. Practice demo (1 hour)

**Ideal Prep (6 hours over weekend):**
1. Follow the full timeline above

---

## 🚨 COMMON MISTAKES TO AVOID

❌ **Don't:** Jump straight into technical details without asking clarifying questions  
✅ **Do:** "Before I answer, can I clarify the time period and data sources?"

❌ **Don't:** Say "I don't have experience with X"  
✅ **Do:** "I haven't used X specifically, but I've used Y which is similar. I can ramp up quickly."

❌ **Don't:** Apologize for not knowing everything  
✅ **Do:** Show how you'd figure it out: "I'd start by checking the documentation and running a test query..."

❌ **Don't:** Over-complicate answers  
✅ **Do:** KISS (Keep It Simple, Stupid) - start broad, then add detail if asked

---

## 💪 YOUR STRENGTHS (Remind Yourself)

✅ 7 years data engineering experience  
✅ Built exactly this at Find.co (multi-source reconciliation)  
✅ Production Airflow expertise (22 DAGs)  
✅ GA4 implementation across 10 brands  
✅ Stakeholder communication (200+ users)  
✅ Fresh, working demo project  

**You're not hoping. You're showing why you're the right fit.**

---

## 📞 FINAL CHECKLIST

**Before Interview:**
- [ ] Project runs successfully
- [ ] Airflow UI accessible (http://localhost:8080)
- [ ] Cheat sheet open or printed
- [ ] Resume and job description nearby
- [ ] Questions for Amos prepared
- [ ] Video/audio working
- [ ] Water, pen, paper ready

**During Interview:**
- [ ] Ask clarifying questions
- [ ] Think out loud (show your process)
- [ ] Reference your project and Find.co
- [ ] Keep answers concise (2-3 minutes max)
- [ ] Show enthusiasm and curiosity

**After Interview:**
- [ ] Send thank you email within 24 hours
- [ ] Reference specific discussion points
- [ ] Reiterate your fit for the role

---

## 🎯 ONE SENTENCE SUMMARY

**You have:** Production experience + Working demo + Clear communication skills = Exactly what they need

---

## 🚀 YOU'RE READY

Everything you need is in these 4 files:
1. **FINAL_INTERVIEW_PREP_SUMMARY.md** - Your game plan
2. **hospitality_booking_reconciliation.tar.gz** - Your proof
3. **INTERVIEW_CHEAT_SHEET.md** - Your reference guide
4. **PROJECT_README.md** - Your technical deep dive

**Follow the plan. Run the code. Practice the pitch. Trust your experience.**

---

**See you on the other side of a successful interview!** 🔥

Mahmud Oyinloye  
"At Find.co, I built this. At The Agile Monkeys, I'll build it better."

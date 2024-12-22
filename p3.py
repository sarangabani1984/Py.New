def quiz(questions):
    score = 0
    for question in questions:
            print(question["Q1"])
            for i in question["options"]:
                  print(i)
            answer = input("Select the following options A/B/C/D: ?").upper()
            if answer == question['Answer']:
                  score +=1
                  print(f"you are correct and your score {score}\n")
            else:
                
                  print("you are wrong the correct answer is",question["Answer"], "\n")
    print(f"You got {score} out of {len(questions)} questions correct.") 
                  

        # for option in question["options"]:

     

questions = [
    {
        "Q1": "What is the national bird of India?",
        "options": ["A. Peacock", "B. Eagle", "C. Crow", "D. Sparrow"],
        "Answer": "A"
    },
    {
        "Q1": "What is the national flower of India?",
        "options": ["A. Rose", "B. Lotus", "C. Marigold", "D. Sunflower"],
        "Answer": "B"
    },
    {
        "Q1": "Which is the national sport of India?",
        "options": ["A. Cricket", "B. Hockey", "C. Football", "D. Kabaddi"],
        "Answer": "B"
    },
    {
        "Q1": "Who is known as the Father of the Nation in India?",
        "options": ["A. Jawaharlal Nehru", "B. Mahatma Gandhi", "C. Subhash Chandra Bose", "D. Bhagat Singh"],
        "Answer": "B"
    },
    {
        "Q1": "Which is the tallest mountain in India?",
        "options": ["A. Mount Everest", "B. Kanchenjunga", "C. Nanda Devi", "D. Annapurna"],
        "Answer": "B"
    }
]

quiz(questions)
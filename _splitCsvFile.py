import chunk

tables = [
    "ME_2022_S1.csv",
    "ME_2022_S2.csv",
    "ME_2023_S1.csv",
    "ME_2023_S2.csv",
    "ME_2024_S1.csv",
    "QA_2021_Q2.csv",
    "QA_2021_Q3.csv",
    "QA_2022_Q1.csv",
    "QA_2022_Q2.csv",
    "QA_2022_Q3.csv",
    "QA_2023_Q1.csv",
    "QA_2023_Q2.csv",
    "QA_2023_Q3.csv",
    "QA_2024_Q1.csv",
    "VTH_ALL.csv"
]

fileEncoding = {
    1: "utf-8",
    2: "iso8859-2"
}[1]

measure = {
    "Kb": 16,
    "Mb": 16640,
    "Gb": 16640*1e3,
    "Tb": 16640*1e6
}["Mb"]

chunksize = 8*measure

preserveHead = True

for i, file in enumerate(tables):
    print(f"[{i+1:02}/{len(tables):02}] Splitting table '{file:<14}'", end="\r")

    inputFile = f"./currentApproach/dataset/{file}"
    outputFile = f"./currentApproach/dataset/{file.replace('.csv', '_%02d.csv')}"

    # print("Reading file", end="...")
    with open(inputFile, 'r', encoding=fileEncoding) as fIn:
        lines = fIn.readlines()
        head = lines[0:1]
        lines = lines[1:]
        # print("Done", end="\r")
        chunksize = int(chunksize)
        for k, i in enumerate(range(0, len(lines), chunksize)):
            print(f"Writing chunk from {i:08} to {i+chunksize:08}", end="\r")
            with open(outputFile % (k+1), 'w', encoding=fileEncoding) as fOut:
                chunkedContent = lines[i:i+chunksize]
                if preserveHead or k == 0:
                    chunkedContent = head + chunkedContent
                fOut.writelines(chunkedContent)

    # print(38*" ", "\rFile splitted!", end="\r")
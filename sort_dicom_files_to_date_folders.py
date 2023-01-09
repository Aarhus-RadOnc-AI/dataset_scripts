# Made with inspiration from:
# Alex Weston
# Digital Innovation Lab, Mayo Clinic

import os
import pydicom  # pydicom is using the gdcm package for decompression
from multiprocessing import Pool
import sys
import shutil

# user specified parameters
src = sys.argv[1]
dst = sys.argv[2]
threads = int(sys.argv[3])

try:
    use_link = bool(int(sys.argv[4]))  # any integer except 0 will be true
except Exception as e:
    print(e)
    print("Failed to parse use_link input. Falling back to copying files")
    use_link = False

def data_loader(src):
    for root, dirs, files in os.walk(src):
        for file in files:
            if ".dcm" in file:  # exclude non-dicoms, good for messy folders
                yield os.path.join(root, file)


def sorter(dicom_loc):
    # read the file
    ds = pydicom.dcmread(dicom_loc, force=True, stop_before_pixels=True)

    # get patient, study, and series information
    patientID = ds.get("PatientID", "NA")
    studyDate = ds.get("StudyDate", "NA")
    studyDescription = ds.get("StudyDescription", "NA")
    seriesDescription = ds.get("SeriesDescription", "NA")

    # generate new, standardized file name
    modality = ds.get("Modality", "NA")
    # studyInstanceUID = ds.get("StudyInstanceUID", "NA")
    seriesInstanceUID = ds.get("SeriesInstanceUID", "NA")
    instanceNumber = str(ds.get("InstanceNumber", "0"))
    fileName = modality + "." + seriesInstanceUID + "." + instanceNumber + ".dcm"

    # save files to a 4-tier nested folder structure
    os.makedirs(os.path.join(dst, patientID, studyDate, seriesDescription), exist_ok=True)

    out_file = os.path.join(dst, patientID, studyDate, seriesDescription, fileName)
    if not os.path.exists(out_file):
        if use_link:
            os.link(dicom_loc, out_file)
        else:
            shutil.copy2(dicom_loc, out_file)


t = Pool(threads)
t.map(sorter, data_loader(src))
t.close()
t.join()

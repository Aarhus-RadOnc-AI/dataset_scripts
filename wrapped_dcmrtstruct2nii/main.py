#!/usr/bin/env python
# coding: utf-8
import json
import sys
import os
import dcmrtstruct2nii
import pydicom
from multiprocessing import Pool

# This script converts dicom to nii with dcmrtstruct2nii with support for multiprocessing. It is compatible with the
# output of the dicom sorting script

def find_dir_with_ct(folder):
    for fol, subs, files in os.walk(folder, followlinks=True):
        for sub in subs:
            sub_path = os.path.join(fol, sub)
            f = os.path.join(sub_path, os.listdir(sub_path)[0])
            with pydicom.filereader.dcmread(f, force=True) as ds:
                if ds.Modality == "CT":
                    return sub_path


def extract_to_nii(file_path, out_folder, xy_scaling_factor):
    try:
        print(f"Converting {file_path}")
        dcmrtstruct2nii.dcmrtstruct2nii(file_path, find_dir_with_ct(file_path.rsplit("/", maxsplit=2)[0]), out_folder, xy_scaling_factor=xy_scaling_factor)
    except Exception as e:
        print(e)
        with open("conversion_errors.log", "a") as f:
            f.write(f"{file_path};{e}\n")

def check_if_rtstruct(f):
    try:
        with pydicom.filereader.dcmread(f, force=True) as ds:
            if ds.Modality == "RTSTRUCT":
                if ds.ApprovalStatus == "APPROVED":
                    print(f"Found RTSTRUCT: {f}")
                    return f
    except Exception as e:
        print(e)

def find_all_rtstructs(dcm):
    ## Get all subs with dicom files inside
    ## Find the shit of rtstructs
    p = Pool(threads)
    rtstructs = p.map(check_if_rtstruct, [os.path.join(fol, f) for fol, subs, files in os.walk(dcm) for f in files])
    p.close()
    p.join()

    return rtstructs

def zip_in_and_out(rtstruct_path, out_path, xy_scaling_factor):
    ## Zip rtstructs with nifti_folder/pt_id
    with pydicom.filereader.dcmread(rtstruct_path, force=True) as ds:
        pid = ds.PatientID
    
        out = os.path.join(out_path, f"{pid}")
        return rtstruct_path, out, xy_scaling_factor

def zip_wrapper(rtstruct_paths, out_path, xy_scaling_factor):
    ## Zip rtstructs with nifti_folder/pt_id
    global threads
    t = Pool(threads)
    results = t.starmap(zip_in_and_out, [(rt_path, out_path, xy_scaling_factor) for rt_path in rtstruct_paths])
    t.close()
    t.join()
    return results

if __name__ == "__main__":
    dcm_folder = sys.argv[1]
    nii_folder = sys.argv[2]
    print(f"RTSTRUCT Dicom folder: {dcm_folder}")
    print(f"Nifti folder: {nii_folder}")
    xy_scaling_factor = int(sys.argv[3])
    print(f"Scaling factor {xy_scaling_factor}")
    try:
        threads = int(sys.argv[4])
        print(f"Threads: {threads}")
    except Exception as e:
        print("Falling back to single thread")
        threads = 1

    try:
        with open(sys.argv[5], "r") as r:
            file_paths = json.loads(r.read())[:10]
            print(f"RTSTRUCT file paths: {file_paths}")
    except Exception as e:
        print(e)

        file_paths = find_all_rtstructs(dcm_folder)
        file_paths = set(file_paths)

        if None in file_paths:
            file_paths.remove(None)

        if not os.path.exists(nii_folder):
            os.makedirs(nii_folder)

        with open(os.path.join(nii_folder, "rtstruct_paths.json"), "w") as f:
            f.write(json.dumps(list(file_paths)))

    ## Convert the shit out of rtstructs
    p = Pool(threads)
    zyps = zip_wrapper(file_paths, nii_folder, xy_scaling_factor)
    conversion = p.starmap(extract_to_nii, zyps)
    p.close()
    p.join()



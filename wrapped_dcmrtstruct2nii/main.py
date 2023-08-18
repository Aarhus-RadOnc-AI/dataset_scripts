#!/usr/bin/env python
# coding: utf-8
import os
import traceback
from multiprocessing import Pool

import dcmrtstruct2nii
import pydicom
import pandas as pd
import json
import argparse


# This script converts dicom to nii with dcmrtstruct2nii with support for multiprocessing. It is compatible with the
# output of the dicom sorting script


def extract_to_nii(row, df, approval_status, nifti_folder):
    if approval_status and row["ApprovalStatus"] != "APPROVED":
        return

    try:
        file_path = row["path"]    
        ct_path = os.path.dirname(df[df["FrameOfReferenceUID"] == row["ReferencedFrameOfReferenceUID"]].iloc[0]["path"])
        out_folder = os.path.join(nifti_folder, row["PatientID"], row["SeriesInstanceUID"])
        os.makedirs(os.path.dirname(out_folder), exist_ok=True)
        print(f"Converting {file_path}")
        dcmrtstruct2nii.dcmrtstruct2nii(file_path,
                                        ct_path,
                                        out_folder,
                                        xy_scaling_factor=xy_scaling_factor,
                                        crop_mask=crop,
                                        convert_original_dicom=convert_image,
                                        structures=inclusion_structures)
    except Exception as e:
        error = f"{e};{traceback.format_exc()}"
        print(error)
        with open(os.path.join(nii_folder, "conversion_errors.log"), "a") as f:
            f.write(f"{file_path};{error}\n")


def check_file(f):
    try:
        with pydicom.dcmread(f, force=True, stop_before_pixels=True) as ds:
            if ds.Modality not in ["CT", "MR", "PT", "RTSTRUCT"]:
                return {}

            meta = {
                "path": f,
                "Modality": ds.Modality,
                "PatientID": ds.PatientID,
                "PatientName": ds.PatientName,
                "SeriesInstanceUID": ds.SeriesInstanceUID
            }

            if ds.Modality == "RTSTRUCT":
                meta["ReferencedFrameOfReferenceUID"] = ds.StructureSetROISequence[0].ReferencedFrameOfReferenceUID
                meta["ApprovalStatus"] = ds.ApprovalStatus
            if ds.Modality in ["CT", "MR"]:
                meta["FrameOfReferenceUID"] = ds.FrameOfReferenceUID
        return meta
    
    except Exception as e:
        print(str(e))
        return {}


def find_all_rtstructs(dcm):
    ## Get all subs with dicom files inside
    ## Find the shit of rtstructs
    p = Pool(threads)
    metas = p.map(check_file,
                        [os.path.join(fol, f) for fol, subs, files in os.walk(dcm, followlinks=True) for f in files])
    p.close()
    p.join()
    df = pd.DataFrame(metas)
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Wrapper of dcmrtstruct2nii')
    parser.add_argument('-d', type=str, help='Path dicoms')
    parser.add_argument('-n', type=str, help='Path to where nifti is output')
    parser.add_argument('-a', type=int, help='XY scaling factor', default=1)
    parser.add_argument('-x', type=int, help='Bool whether to crop mask to ROI', default=False)
    parser.add_argument('-c', type=int, help='Whether to convert dicom image', default=True)
    parser.add_argument('-t', type=int, help='Threads - use with care with high xy scaling factors', default=1)
    parser.add_argument('-p', type=int, help='Convert only RTSTRUCTs with ApprovalStatus=APPROVED', default=False)
    parser.add_argument('-s', nargs='+',
                        help='Structures to convert. Comma seperated with mo spaces. You can use "~" to exclude',
                        default=None)
    parser.add_argument('-j', type=str, help='Path an existing json of all RTSTRUCTS to convert', default=None)

    args = parser.parse_args()

    dcm_folder = args.d
    print(f"RTSTRUCT Dicom folder: {dcm_folder}")

    nii_folder = args.n
    print(f"Nifti folder: {nii_folder}")
    os.makedirs(nii_folder, exist_ok=True)

    xy_scaling_factor = args.a
    print(f"Scaling factor {xy_scaling_factor}")

    crop = bool(args.x)
    print(f"Crop: {str(crop)}")

    convert_image = bool(args.c)
    print(f"Convert image: {str(convert_image)}")

    threads = args.t
    print(f"Threads {threads}")

    approved_only = bool(args.p)
    print(f"Approved only: {approved_only}")

    inclusion_structures = args.s
    print(f"inclusion_structures: {inclusion_structures}")

    rt_files = args.j
    if rt_files:
        try:
            df = pd.read_csv(rt_files)
        except Exception as e:
            print(e)
    else:
        df = find_all_rtstructs(dcm_folder)
        df.to_csv(os.path.join(nii_folder, "dicom_files.csv"))
    
    ## Convert the shit out of rtstructs
    p = Pool(threads)

    conversion = p.starmap(extract_to_nii, [(row, df, approved_only, nii_folder) for i, row in df[df["Modality"] == "RTSTRUCT"].iterrows()])
    p.close()
    p.join()

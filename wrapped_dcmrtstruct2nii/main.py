#!/usr/bin/env python
# coding: utf-8
import os
import traceback
from multiprocessing import Pool

import dcmrtstruct2nii
import pydicom

import json
import argparse


# This script converts dicom to nii with dcmrtstruct2nii with support for multiprocessing. It is compatible with the
# output of the dicom sorting script

def find_dir_with_ct(folder, uid):
    for fol, subs, files in os.walk(folder, followlinks=True):
        for file in files:
            f = os.path.join(fol, file)
            try:
                with pydicom.dcmread(f, force=True, stop_before_pixels=True) as ds:
                    if check_cts_explicitly:
                        if ds.FrameOfReferenceUID == uid and ds.Modality == "CT":
                            return os.path.dirname(f)
                    elif ds.Modality == "CT":
                        return os.path.dirname(f)

            except AttributeError:
                pass


def extract_to_nii(file_path, out_folder):
    try:
        ds = pydicom.dcmread(file_path, force=True, stop_before_pixels=True)
        for i, structure in enumerate(ds.StructureSetROISequence):
            uid = structure[0x30060024].value
            
            p = os.path.abspath(os.path.join(os.path.dirname(file_path), look_ct_path))

            ct_path = find_dir_with_ct(p, uid)
            if ct_path:
                break

        else:
            raise Exception(f"{file_path}, CT not found")

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


def check_if_rtstruct(f, approved_only):
    try:
        with pydicom.dcmread(f, force=True, stop_before_pixels=True) as ds:
            if ds.Modality == "RTSTRUCT":
                if approved_only:
                    if ds.ApprovalStatus == "APPROVED":
                        print(f"Found RTSTRUCT: {f}")
                        return f
                else:
                    print(f"Found RTSTRUCT: {f}")
                    return f

    except Exception as e:
        pass


def find_all_rtstructs(dcm):
    ## Get all subs with dicom files inside
    ## Find the shit of rtstructs
    p = Pool(threads)
    rtstructs = p.starmap(check_if_rtstruct,
                          [(os.path.join(fol, f), approved_only) for fol, subs, files in os.walk(dcm, followlinks=True)
                           for f in files])
    p.close()
    p.join()

    return rtstructs


def zip_in_and_out(rtstruct_path, out_path):
    ## Zip rtstructs with nifti_folder/pt_id
    with pydicom.filereader.dcmread(rtstruct_path, force=True) as ds:
        series = ds.SeriesInstanceUID
        fol = os.path.dirname(rtstruct_path)
        out = os.path.join(out_path, fol, series)

    os.makedirs(out, exist_ok=True)

    return rtstruct_path, out


def zip_wrapper(rtstruct_paths, out_path):
    ## Zip rtstructs with nifti_folder/pt_id
    t = Pool(threads)
    results = t.starmap(zip_in_and_out, [(rt_path, out_path) for rt_path in rtstruct_paths])
    t.close()
    t.join()
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Wrapper of dcmrtstruct2nii')
    parser.add_argument('-d', type=str, help='Path dicoms')
    parser.add_argument('-n', type=str, help='Path to where nifti is output')
    parser.add_argument('-a', type=int, help='XY scaling factor', default=1)
    parser.add_argument('-x', type=int, help='Bool whether to crop mask to ROI', default=False)
    parser.add_argument('-c', type=int, help='Whether to convert dicom image', default=True)
    parser.add_argument('-t', type=int, help='Threads - use with care with high xy scaling factors', default=1)
    parser.add_argument('-p', type=int, help='Convert only RTSTRUCTs with ApprovalStatus=Approved', default=False)
    parser.add_argument('-s', nargs='+',
                        help='Structures to convert. Comma seperated with mo spaces. You can use "~" to exclude',
                        default=None)
    parser.add_argument('-j', type=str, help='Path an existing json of all RTSTRUCTS to convert', default=None)
    parser.add_argument('-k', type=str, help='Relative path to where to look for CT from rtstruct. Default is ".."', default="..")
    parser.add_argument('-m', type=int, help='Check CTs explicitely for match', default=1)

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

    check_cts_explicitly = bool(args.m)
    print(f"Check CT eplicitly: {check_cts_explicitly}")

    look_ct_path = args.k
    print(f"Relative path to cts: {look_ct_path}")

    rt_files = args.j
    if rt_files:
        try:
            with open(rt_files, "r") as r:
                file_paths = json.loads(r.read())
                print(f"RTSTRUCT file paths: {file_paths}")
        except Exception as e:
            print(e)

    else:
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
    zyps = zip_wrapper(file_paths, nii_folder)
    conversion = p.starmap(extract_to_nii, zyps)
    p.close()
    p.join()

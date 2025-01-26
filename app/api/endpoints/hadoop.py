from fastapi import APIRouter, UploadFile, Depends, HTTPException
from fastapi.responses import JSONResponse
from app.services.hadoop_service import HadoopAPIClient
from app.models.request_models import (
    HadoopFileUpload,
    HadoopFileDelete
)
from app.models.response_models import (
    HadoopFileResponse,
    ErrorResponse, BaseResponse
)
from app.core.security import api_key_auth

hadoop_router = APIRouter(
    prefix="/hadoop",
    tags=["Hadoop Operations"],
    dependencies=[Depends(api_key_auth)]
)


@hadoop_router.post(
    "/upload",
    response_model=HadoopFileResponse,
    responses={
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse}
    }
)
async def upload_file(
        request: HadoopFileUpload,
        file: UploadFile
):
    """通过REST API上传文件到HDFS"""
    try:
        client = HadoopAPIClient()
        result = await client.upload_file(
            hdfs_path=request.hdfs_path,
            file_content=await file.read(),
            overwrite=request.overwrite,
            replication=request.replication,
            blocksize=request.blocksize
        )

        return HadoopFileResponse(
            success=True,
            hdfs_path=result["hdfs_path"],
            file_size=result["file_size"],
            block_size=result["block_size"],
            replication=result["replication"]
        )

    except client.HDFSNotFoundError as e:
        raise HTTPException(404, detail={
            "error_code": "HDFS_PATH_NOT_FOUND",
            "detail": str(e)
        })
    except client.HDFSConflictError as e:
        raise HTTPException(409, detail={
            "error_code": "HDFS_PATH_EXISTS",
            "detail": str(e)
        })
    except Exception as e:
        raise HTTPException(500, detail={
            "error_code": "HDFS_OPERATION_FAILED",
            "detail": str(e)
        })


@hadoop_router.delete(
    "/delete",
    response_model=BaseResponse,
    responses={404: {"model": ErrorResponse}}
)
async def delete_path(request: HadoopFileDelete):
    """删除HDFS路径"""
    try:
        client = HadoopAPIClient()
        await client.delete_path(
            hdfs_path=request.hdfs_path,
            recursive=request.recursive
        )
        return BaseResponse(
            success=True,
            message=f"Successfully deleted {request.hdfs_path}"
        )
    except client.HDFSNotFoundError as e:
        raise HTTPException(404, detail={
            "error_code": "HDFS_PATH_NOT_FOUND",
            "detail": str(e)
        })
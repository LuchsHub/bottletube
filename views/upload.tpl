% include('header.tpl', title=name)

% setdefault('error_messages', [])
% for m in error_messages:
  <div class="alert alert-danger" role="alert">
    {{m}}
  </div>
% end

<div class="container text-left">
    <form method="POST" action="/upload" enctype="multipart/form-data">
      <div class="form-group mt-2 mb-2">
        <label for="formGroupCategoryInput">Category</label>
        <input type="text" class="form-control" id="formGroupCategoryInput" placeholder="Enter Category" name="category">
      </div>
      <div class="form-group mt-2 mb-2">
        <label for="formGroupFileInput">Select File</label>
        <input type="file" class="form-control-file" id="formGroupFileInput" placeholder="Select File" name="file_upload">
      </div>
      <button type="submit" class="btn btn-primary">Submit</button>
    </form>
</div>

% include('footer.tpl', title=name)

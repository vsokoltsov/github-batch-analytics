output "streamlit_iam_user_name" { value = aws_iam_user.streamlit.name }
output "streamlit_iam_user_arn" { value = aws_iam_user.streamlit.arn }
output "streamlit_access_key_id" {
  value     = aws_iam_access_key.streamlit.id
  sensitive = true
}
output "streamlit_secret_access_key" {
  value     = aws_iam_access_key.streamlit.secret
  sensitive = true
}
